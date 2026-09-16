"""The backend seam: argv shapes, freedom mapping, honest enforcement, stream normalisation.

Stream fixtures below are events captured from real runs, not invented.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polybridge import backends
from polybridge.backends import EFFORTS, FREEDOMS, Accumulator, Capabilities, ReasoningEffort
from polybridge.backends.claude import ALLOWED_TOOLS, DISALLOWED_TOOLS, FORBIDDEN_FLAGS, ClaudeBackend
from polybridge.backends.claude import UnsafeInvocationError as ClaudeUnsafe
from polybridge.backends.codex import BINARY as CODEX_BINARY
from polybridge.backends.codex import EFFORT_KEY as CODEX_EFFORT_KEY
from polybridge.backends.codex import (
    NETWORK_DISABLE_PAIR,
    NETWORK_ENABLE_PAIR,
    NEVER_ASK,
    CodexBackend,
)
from polybridge.backends.codex import UnsafeInvocationError as CodexUnsafe
from polybridge.backends.opencode import REJECTED_FLAGS, OpencodeBackend
from polybridge.backends.opencode import UnsafeInvocationError as OpencodeUnsafe
from polybridge.backends.vibe import AGENTS as VIBE_AGENTS
from polybridge.backends.vibe import REJECTED_FLAGS as VIBE_REJECTED_FLAGS
from polybridge.backends.vibe import VibeBackend
from polybridge.backends.vibe import UnsafeInvocationError as VibeUnsafe

REPO = Path("/tmp/repo")
SESSION = "11111111-1111-1111-1111-111111111111"

ALL = [ClaudeBackend(), CodexBackend(), OpencodeBackend(), VibeBackend()]


class _NoEffortBackend:
    """A minimal Backend double whose capabilities decline reasoning_effort outright.

    None of the three real backends declare accepts_parameter=False, so this stands in for the
    shape check_reasoning_effort must still refuse — mirroring how reject_turn_cap is exercised
    against CodexBackend/OpencodeBackend, except no registered backend plays that role here.
    """

    name = "no-effort"
    capabilities = Capabilities(
        chooses_session_id=False,
        supports_turn_cap=False,
        reports_cost_usd=False,
        os_sandbox=False,
        per_command_deny=False,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=False, levels=(), native_flag="",
            accepted_in_real_run=False, levels_change_behaviour=False,
        ),
    )


class _PartialEffortBackend:
    """A Backend double that accepts only some of EFFORTS, to exercise the "names what it has"
    branch of check_reasoning_effort — no real backend restricts the vocabulary this way."""

    name = "partial-effort"
    capabilities = Capabilities(
        chooses_session_id=False,
        supports_turn_cap=False,
        reports_cost_usd=False,
        os_sandbox=False,
        per_command_deny=False,
        supports_model_selection=True,
        reasoning_effort=ReasoningEffort(
            accepts_parameter=True,
            levels=("low", "medium"),
            native_flag="--fake",
            accepted_in_real_run=True,
            levels_change_behaviour=True,
            caveats=("fake",),
        ),
    )


def start(backend, **kwargs):
    session_id = SESSION if backend.capabilities.chooses_session_id else None
    args = {
        "repo": REPO,
        "freedom": "write_in_repo",
        "session_id": session_id,
        "model": None,
        "max_turns": None,
        "reasoning_effort": None,
    } | kwargs
    return backend.build_start_argv("do a thing", **args)


def with_extra_options(argv: list[str], *extra: str) -> list[str]:
    """Insert options into the option region, i.e. before the `--` separator."""
    cut = argv.index("--") if "--" in argv else len(argv)
    return [*argv[:cut], *extra, *argv[cut:]]


def resume(backend, **kwargs):
    args = {
        "repo": REPO,
        "freedom": "write_in_repo",
        "session_id": "abc-123",
        "model": None,
        "max_turns": None,
        "reasoning_effort": None,
    } | kwargs
    return backend.build_resume_argv("more", **args)


# --- registry ------------------------------------------------------------------------------


def test_every_backend_is_registered() -> None:
    assert sorted(backends.BACKENDS) == ["claude", "codex", "opencode", "vibe"]


def test_unknown_backend_is_rejected() -> None:
    with pytest.raises(backends.UnknownBackend, match="unknown backend"):
        backends.get("nope")


def test_describe_covers_every_freedom(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(backends, "is_installed", lambda backend: False)
    described = backends.describe(ClaudeBackend())

    assert described["installed"] is False
    assert described["version"] is None
    assert sorted(described["freedoms"]) == sorted(FREEDOMS)


# --- shared invariants ---------------------------------------------------------------------


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("freedom", FREEDOMS)
def test_every_backend_builds_an_argv_it_considers_safe(backend, freedom: str) -> None:
    backend.assert_safe(start(backend, freedom=freedom), freedom)
    backend.assert_safe(resume(backend, freedom=freedom), freedom)


# Two (backend, freedom-pair) combinations are *deliberately* indistinguishable from argv alone —
# not a hole in assert_safe, but a documented property of how those backends map freedoms to
# mechanisms. See test_known_freedom_collapses_produce_byte_identical_argv, which pins the collapse
# itself, and each backend's own `_MODE_CAVEATS["publish"]` / `AGENTS["publish"]` comment.
KNOWN_COLLAPSED_FREEDOM_PAIRS: frozenset[tuple[str, frozenset[str]]] = frozenset(
    {
        ("opencode", frozenset({"write_in_repo", "publish"})),
        ("vibe", frozenset({"publish", "unrestricted"})),
    }
)


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("built_freedom", FREEDOMS)
@pytest.mark.parametrize("claimed_freedom", FREEDOMS)
def test_assert_safe_refuses_an_argv_built_for_a_different_freedom(
    backend, built_freedom: str, claimed_freedom: str
) -> None:
    """The whole point of widening assert_safe: it must check the argv against the freedom the
    caller actually asked for, not merely that the argv is internally consistent for *some*
    freedom. Without this, an argv built for read_only would pass a check told unrestricted, and
    vice versa — this is deliberately the test that fails if that check is ever removed or weakened
    back to a membership check (see the mutation-check step in the plan).

    Excludes exactly the pairs in KNOWN_COLLAPSED_FREEDOM_PAIRS: assert_safe genuinely cannot refuse
    a mismatch there, because there is no argv difference to detect — a documented property, pinned
    by test_known_freedom_collapses_produce_byte_identical_argv, not a silent gap in this test.
    """
    if built_freedom == claimed_freedom:
        pytest.skip("same-freedom case is covered by test_every_backend_builds_an_argv_it_considers_safe")
    if (backend.name, frozenset({built_freedom, claimed_freedom})) in KNOWN_COLLAPSED_FREEDOM_PAIRS:
        pytest.skip(
            f"{backend.name} deliberately collapses {built_freedom!r} and {claimed_freedom!r}"
        )
    for argv in (
        start(backend, freedom=built_freedom),
        resume(backend, freedom=built_freedom),
    ):
        with pytest.raises(RuntimeError):
            backend.assert_safe(argv, claimed_freedom)


@pytest.mark.parametrize(
    ("backend", "freedom_a", "freedom_b"),
    [
        (OpencodeBackend(), "write_in_repo", "publish"),
        (VibeBackend(), "publish", "unrestricted"),
    ],
    ids=["opencode-write_in_repo-publish", "vibe-publish-unrestricted"],
)
def test_known_freedom_collapses_produce_byte_identical_argv(backend, freedom_a, freedom_b) -> None:
    """Pins the collapse deliberately rather than leaving it an accident of the current mapping —
    see KNOWN_COLLAPSED_FREEDOM_PAIRS, which excludes exactly these two pairs from the cross-freedom
    refusal test above."""
    assert start(backend, freedom=freedom_a) == start(backend, freedom=freedom_b)
    assert resume(backend, freedom=freedom_a) == resume(backend, freedom=freedom_b)
    caveats = " ".join(backend.enforcement(freedom_a).caveats) + " ".join(
        backend.enforcement(freedom_b).caveats
    )
    assert "cannot tell these two freedoms apart" in caveats


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_assert_safe_rejects_an_unknown_freedom_rather_than_a_key_error(backend) -> None:
    """base.check_freedom must be consulted before any dict keyed by freedom (PERMISSION_MODES,
    SANDBOX_MODES, AGENTS, ...) is indexed, or an unrecognised value would surface as a bare
    KeyError instead of the same UnsupportedCapability every other unknown-value path raises."""
    argv = start(backend)
    with pytest.raises(backends.UnsupportedCapability, match="unknown freedom"):
        backend.assert_safe(argv, "bogus-freedom")  # type: ignore[arg-type]


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("prompt", ["", "   "])
def test_empty_prompts_are_rejected(backend, prompt: str) -> None:
    session_id = SESSION if backend.capabilities.chooses_session_id else None
    with pytest.raises(ValueError):
        backend.build_start_argv(
            prompt, repo=REPO, freedom="write_in_repo", session_id=session_id, model=None,
            max_turns=None, reasoning_effort=None,
        )


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_session_id_handling_matches_the_declared_capability(backend) -> None:
    """A backend that mints its own id must refuse one, and vice versa."""
    with pytest.raises(ValueError):
        backend.build_start_argv(
            "x",
            repo=REPO,
            freedom="write_in_repo",
            # Deliberately the wrong way round for this backend.
            session_id=None if backend.capabilities.chooses_session_id else SESSION,
            model=None,
            max_turns=None,
            reasoning_effort=None,
        )


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("freedom", FREEDOMS)
def test_enforcement_never_overclaims(backend, freedom: str) -> None:
    """The whole point of the abstraction: a report must not exceed what the backend can do."""
    enforcement = backend.enforcement(freedom)
    caps = backend.capabilities

    assert enforcement.freedom == freedom
    if not caps.os_sandbox:
        assert enforcement.os_enforced is False
        assert enforcement.writes_confined is False
    if not caps.per_command_deny:
        assert enforcement.direct_commit_commands_denied is False
    # Neither backend can actually prevent a commit: Claude's deny patterns are evadable and Codex
    # has none at all. A True here would be a promise nothing keeps.
    assert enforcement.commit_push_blocked is False
    assert enforcement.caveats, "a caveat-free claim is almost certainly overstated"


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("freedom", FREEDOMS)
def test_confined_writes_say_where_they_may_still_land(backend, freedom: str) -> None:
    """"Confined" must not be read as "repo only" — codex also permits temporary directories."""
    enforcement = backend.enforcement(freedom)

    if enforcement.writes_confined and freedom != "read_only":
        assert enforcement.writable_roots, "confinement must state what remains writable"
    if freedom == "read_only" and enforcement.writes_confined:
        assert enforcement.writable_roots == (), "read-only permits no writes at all"


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("freedom", FREEDOMS)
def test_publish_attempts_allowed_by_polybridge_matches_the_ladder(backend, freedom: str) -> None:
    """False at read_only/write_in_repo, True at publish/unrestricted — for all four backends,
    even though what that True actually amounts to varies wildly (see the mechanism table in
    CLAUDE.md / README.md). Never named `publishing_permitted`: see the field's own docstring for
    why that name would overclaim."""
    expected = freedom in ("publish", "unrestricted")
    enforcement = backend.enforcement(freedom)
    assert enforcement.publish_attempts_allowed_by_polybridge is expected


# Exact measured table. claude/opencode/vibe impose no sandbox at all, so network reachability is
# never something polybridge controls there — "not_controlled" at every freedom. codex alone has an
# OS sandbox with a measured, freedom-dependent effect on network reachability.
EXPECTED_NETWORK_ACCESS: dict[str, dict[str, str]] = {
    "claude": dict.fromkeys(FREEDOMS, "not_controlled"),
    "codex": {
        "read_only": "blocked",
        "write_in_repo": "blocked",
        "publish": "enabled",
        "unrestricted": "unrestricted",
    },
    "opencode": dict.fromkeys(FREEDOMS, "not_controlled"),
    "vibe": dict.fromkeys(FREEDOMS, "not_controlled"),
}


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("freedom", FREEDOMS)
def test_network_access_matches_the_measured_table(backend, freedom: str) -> None:
    expected = EXPECTED_NETWORK_ACCESS[backend.name][freedom]
    assert backend.enforcement(freedom).network_access == expected


def test_claude_denies_direct_commit_commands_without_claiming_to_block_them() -> None:
    enforcement = ClaudeBackend().enforcement("write_in_repo")

    assert enforcement.direct_commit_commands_denied is True
    assert enforcement.commit_push_blocked is False


def test_codex_confinement_includes_temp_directories_not_just_the_repo() -> None:
    """Measured from codex's own reported sandbox: [workdir, /tmp, $TMPDIR]."""
    roots = CodexBackend().enforcement("write_in_repo").writable_roots

    assert any("/tmp" in root for root in roots)


def test_codex_unrestricted_admits_it_enforces_nothing() -> None:
    enforcement = CodexBackend().enforcement("unrestricted")

    assert enforcement.os_enforced is False
    assert enforcement.writes_confined is False


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_model_is_optional_and_passed_through(backend) -> None:
    if not backend.capabilities.supports_model_selection:
        pytest.skip(f"{backend.name} has no model-selection flag at all")
    assert "sonnet" not in " ".join(start(backend))
    assert "sonnet" in " ".join(start(backend, model="sonnet"))


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_a_backend_with_no_model_selection_refuses_a_model_rather_than_dropping_it(backend) -> None:
    """Mirrors test_model_is_optional_and_passed_through's skip: the other side of that branch."""
    if backend.capabilities.supports_model_selection:
        pytest.skip(f"{backend.name} does support model selection")
    with pytest.raises(backends.UnsupportedCapability, match="no model selection flag"):
        start(backend, model="mistral-medium")
    with pytest.raises(backends.UnsupportedCapability, match="no model selection flag"):
        resume(backend, model="mistral-medium")


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_supports_model_selection_matches_what_was_actually_measured(backend) -> None:
    expected = backend.name != "vibe"
    assert backend.capabilities.supports_model_selection is expected


def test_reject_model_passes_none_through_regardless_of_capability() -> None:
    for backend in ALL:
        backends.reject_model(backend, None)


def test_reject_model_refuses_a_model_on_a_backend_that_declares_it_cannot() -> None:
    with pytest.raises(backends.UnsupportedCapability, match="no model selection flag"):
        backends.reject_model(VibeBackend(), "mistral-medium")


def test_reject_model_is_fine_where_supported() -> None:
    backends.reject_model(ClaudeBackend(), "sonnet")
    backends.reject_model(CodexBackend(), None)


# --- reasoning effort: shared across all three backends --------------------------------------


def _effort_marker(backend_name: str, level: str) -> tuple[str, str] | str:
    """The token(s) that must appear in argv when `level` was requested, in that backend's own
    spelling — claude and opencode take it as a flag/value pair, codex as a `-c key="value"` pair.
    """
    if backend_name == "claude":
        return ("--effort", level)
    if backend_name == "codex":
        return f'{CODEX_EFFORT_KEY}="{level}"'
    if backend_name == "opencode":
        return ("--variant", level)
    raise AssertionError(f"no effort marker known for {backend_name!r}")


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_reasoning_effort_is_optional_and_absent_by_default(backend) -> None:
    for argv in (start(backend), resume(backend)):
        assert "--effort" not in argv
        assert "--variant" not in argv
        assert not any(token.startswith(f"{CODEX_EFFORT_KEY}=") for token in argv)


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("level", EFFORTS)
def test_reasoning_effort_reaches_argv_verbatim_under_the_backends_own_flag(
    backend, level: str
) -> None:
    """On start and on resume alike — a caller cannot tell from the argv shape which path ran.

    Guarded on accepts_parameter: vibe declares it False and has no native flag at all, so there is
    no marker to look for — check_reasoning_effort_refuses_it_outright covers that backend instead.
    """
    if not backend.capabilities.reasoning_effort.accepts_parameter:
        pytest.skip(f"{backend.name} has no reasoning effort control at all")
    for argv in (start(backend, reasoning_effort=level), resume(backend, reasoning_effort=level)):
        marker = _effort_marker(backend.name, level)
        if isinstance(marker, tuple):
            flag, value = marker
            assert argv[argv.index(flag) + 1] == value
        else:
            assert marker in argv


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("value", ["ultra", "max", "minimal", "bogus", ""])
def test_a_non_canonical_reasoning_effort_is_refused_rather_than_forwarded(
    backend, value: str
) -> None:
    """Not just a backend-specific quirk: outside EFFORTS is refused before any backend is asked."""
    with pytest.raises(backends.UnsupportedCapability, match="unknown reasoning_effort"):
        start(backend, reasoning_effort=value)


def test_a_backend_that_does_not_accept_effort_refuses_it_rather_than_dropping_it() -> None:
    """Mirrors the max_turns precedent — no registered backend declines, so a double stands in."""
    with pytest.raises(backends.UnsupportedCapability, match="no reasoning effort control"):
        backends.check_reasoning_effort(_NoEffortBackend(), "high")


def test_a_level_outside_the_backends_own_set_is_refused_naming_what_it_has() -> None:
    with pytest.raises(backends.UnsupportedCapability, match=r"it accepts \['low', 'medium'\]"):
        backends.check_reasoning_effort(_PartialEffortBackend(), "xhigh")


def test_check_reasoning_effort_passes_none_through() -> None:
    for backend in [*ALL, _NoEffortBackend()]:
        backends.check_reasoning_effort(backend, None)


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_levels_change_behaviour_implies_accepted_in_a_real_run(backend) -> None:
    """The stronger claim cannot stand without the weaker one it builds on — mirrors why
    commit_push_blocked could never be True while direct_commit_commands_denied was False."""
    reasoning = backend.capabilities.reasoning_effort
    if reasoning.levels_change_behaviour:
        assert reasoning.accepted_in_real_run


# Exact expected values per backend, so a regression to `False`/`False` across the board — or to
# an unrelated caveat standing in for the real claim — cannot pass silently. Only opencode has a
# cross-level behavioural measurement (see its `_EFFORT_CAVEAT`); claude and codex have acceptance
# without a behavioural comparison (codex's own stream reports zero reasoning output tokens at
# every level, so no such comparison is even observable there).
EXPECTED_REASONING_EFFORT_FLAGS: dict[str, tuple[bool, bool]] = {
    "claude": (True, False),
    "codex": (True, False),
    "opencode": (True, True),
    # vibe has no effort flag at all — see VibeBackend's _EFFORT_CAVEAT for why it is not attempted.
    "vibe": (False, False),
}


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_reasoning_effort_flags_match_what_was_actually_measured_per_backend(backend) -> None:
    accepted_in_real_run, levels_change_behaviour = EXPECTED_REASONING_EFFORT_FLAGS[backend.name]
    reasoning = backend.capabilities.reasoning_effort

    assert reasoning.accepted_in_real_run is accepted_in_real_run
    assert reasoning.levels_change_behaviour is levels_change_behaviour


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_a_backend_with_known_effort_limits_names_them_in_caveats(backend) -> None:
    """Mirrors test_enforcement_never_overclaims: every real backend's effort support has a known
    gap (claude: CLI-side degradation outside polybridge's own check; codex: no cross-level output
    signal; opencode: per-model support), so accepting the parameter at all must carry a caveat."""
    reasoning = backend.capabilities.reasoning_effort
    if reasoning.accepts_parameter:
        assert reasoning.caveats, "a caveat-free claim here is almost certainly overstated"


def test_opencode_reasoning_effort_caveat_states_the_per_model_gap() -> None:
    caveats = " ".join(OpencodeBackend().capabilities.reasoning_effort.caveats)
    assert "variant" in caveats.lower()
    assert "model" in caveats.lower()


@pytest.mark.parametrize("backend", [*ALL, _NoEffortBackend()], ids=lambda b: b.name)
def test_reasoning_effort_accepts_parameter_agrees_with_whether_levels_is_empty(backend) -> None:
    reasoning = backend.capabilities.reasoning_effort
    assert reasoning.accepts_parameter == bool(reasoning.levels)


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_capabilities_as_dict_serializes_reasoning_effort_to_json_safe_lists(backend) -> None:
    data = backend.capabilities.as_dict()["reasoning_effort"]
    assert isinstance(data["levels"], list)
    assert isinstance(data["caveats"], list)
    json.dumps(data)  # must not raise: no bare tuple/NamedTuple left inside


# --- claude specifics ----------------------------------------------------------------------


def test_claude_denies_commit_and_push_on_every_path() -> None:
    for argv in (start(ClaudeBackend()), resume(ClaudeBackend())):
        assert argv[argv.index("--disallowedTools") + 1] == DISALLOWED_TOOLS
        assert "Bash(git commit:*)" in DISALLOWED_TOOLS
        assert "Bash(git push:*)" in DISALLOWED_TOOLS


def test_claude_requires_verbose_because_the_cli_does() -> None:
    """`-p --output-format stream-json` refuses to start without it."""
    assert "--verbose" in start(ClaudeBackend())


def test_claude_start_and_resume_use_mutually_exclusive_session_flags() -> None:
    started, resumed = start(ClaudeBackend()), resume(ClaudeBackend())
    assert "--session-id" in started and "--resume" not in started
    assert "--resume" in resumed and "--session-id" not in resumed


@pytest.mark.parametrize("flag", FORBIDDEN_FLAGS)
def test_claude_refuses_flags_that_would_isolate_the_agent(flag: str) -> None:
    """Dispatched agents must keep inheriting the user's MCP servers, hooks and CLAUDE.md."""
    argv = start(ClaudeBackend())
    assert flag not in argv
    with pytest.raises(ClaudeUnsafe, match="cut the dispatched agent off"):
        ClaudeBackend().assert_safe(with_extra_options(argv, flag), "write_in_repo")


def test_claude_rejects_a_weakened_deny_list() -> None:
    argv = start(ClaudeBackend())
    argv[argv.index("--disallowedTools") + 1] = "Bash(git push:*)"
    with pytest.raises(ClaudeUnsafe):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_a_claude_prompt_that_looks_like_a_flag_is_not_mistaken_for_one() -> None:
    argv = ClaudeBackend().build_start_argv(
        "--disallowedTools", repo=REPO, freedom="write_in_repo", session_id=SESSION, model=None,
        max_turns=None, reasoning_effort=None,
    )
    ClaudeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize(
    "prompt",
    [
        "--dangerously-skip-permissions",
        "--permission-mode=bypassPermissions",
        "--disallowed-tools=Bash(git commit:*)",
        "--effort",
        "--",
    ],
)
def test_a_claude_prompt_equal_to_a_real_flag_name_still_passes_once_separated(
    prompt: str,
) -> None:
    """Measured against the real claude CLI: `-p`/`--print` is a boolean flag and `prompt` is a
    separate declared positional, so without a `--` separator a prompt token that exactly matches a
    real claude option name is parsed by claude as that option, not as text (see the module
    docstring). Once the builder places `--` before the prompt, claude reads it as literal text
    regardless of content — this is the case that must keep working."""
    start_argv = ClaudeBackend().build_start_argv(
        prompt, repo=REPO, freedom="write_in_repo", session_id=SESSION, model=None,
        max_turns=None, reasoning_effort=None,
    )
    assert start_argv[-2:] == ["--", prompt]
    ClaudeBackend().assert_safe(start_argv, "write_in_repo")

    resume_argv = ClaudeBackend().build_resume_argv(
        prompt, repo=REPO, freedom="write_in_repo", session_id="abc-123", model=None,
        max_turns=None, reasoning_effort=None,
    )
    assert resume_argv[-2:] == ["--", prompt]
    ClaudeBackend().assert_safe(resume_argv, "write_in_repo")


def test_claude_refuses_an_argv_without_a_separator_before_the_prompt() -> None:
    """The pre-`--` shape this backend used to build: prompt as a fixed positional right after
    `-p`. Measured to be unsafe (see module docstring), so assert_safe must refuse it outright even
    though every flag in it is otherwise well-formed."""
    argv = [
        "claude", "-p", "--dangerously-skip-permissions", "--output-format", "stream-json",
        "--verbose", "--permission-mode", "acceptEdits", "--disallowedTools", DISALLOWED_TOOLS,
        "--session-id", SESSION,
    ]
    with pytest.raises(ClaudeUnsafe, match="separator"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_refuses_extra_tokens_after_the_prompt_separator() -> None:
    """`with_extra_options` inserts before `--`; appending directly after simulates a stray
    positional riding along with the prompt, which claude only ever declares one of."""
    argv = start(ClaudeBackend()) + ["extra positional"]
    with pytest.raises(ClaudeUnsafe, match="expected exactly one positional"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_turn_cap_is_emitted() -> None:
    argv = start(ClaudeBackend(), max_turns=7)
    assert argv[argv.index("--max-turns") + 1] == "7"


def test_claude_rejects_a_duplicate_effort_flag_whose_second_value_would_win() -> None:
    argv = with_extra_options(start(ClaudeBackend(), reasoning_effort="low"), "--effort", "high")
    with pytest.raises(ClaudeUnsafe, match="--effort appears"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_rejects_an_unexpected_effort_value_reaching_assert_safe_directly() -> None:
    argv = with_extra_options(start(ClaudeBackend()), "--effort", "bogus")
    with pytest.raises(ClaudeUnsafe, match="unexpected --effort"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("freedom", ["read_only", "write_in_repo"])
def test_claude_deny_patterns_present_at_read_only_and_write_in_repo(freedom: str) -> None:
    argv = start(ClaudeBackend(), freedom=freedom)
    assert argv[argv.index("--disallowedTools") + 1] == DISALLOWED_TOOLS
    assert "--allowedTools" not in argv


@pytest.mark.parametrize("freedom", ["publish", "unrestricted"])
def test_claude_deny_patterns_absent_at_publish_and_unrestricted(freedom: str) -> None:
    argv = start(ClaudeBackend(), freedom=freedom)
    assert "--disallowedTools" not in argv


def test_claude_publish_carries_the_allowlist_and_no_deny_patterns() -> None:
    argv = start(ClaudeBackend(), freedom="publish")
    assert argv[argv.index("--allowedTools") + 1] == ALLOWED_TOOLS["publish"]
    assert "Bash(git commit:*)" in ALLOWED_TOOLS["publish"]
    assert "Bash(git push:*)" in ALLOWED_TOOLS["publish"]
    assert "Bash(gh pr create:*)" in ALLOWED_TOOLS["publish"]
    ClaudeBackend().assert_safe(argv, "publish")


@pytest.mark.parametrize("freedom", ["read_only", "write_in_repo", "unrestricted"])
def test_claude_allowlist_absent_outside_publish(freedom: str) -> None:
    assert "--allowedTools" not in start(ClaudeBackend(), freedom=freedom)


def test_claude_unrestricted_has_neither_denies_nor_allowlist() -> None:
    argv = start(ClaudeBackend(), freedom="unrestricted")
    assert "--disallowedTools" not in argv
    assert "--allowedTools" not in argv
    ClaudeBackend().assert_safe(argv, "unrestricted")


def test_claude_assert_safe_rejects_an_allowlist_present_outside_publish() -> None:
    argv = with_extra_options(start(ClaudeBackend(), freedom="write_in_repo"), "--allowedTools",
                               ALLOWED_TOOLS["publish"])
    with pytest.raises(ClaudeUnsafe, match="allowedTools"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_assert_safe_rejects_publish_missing_its_allowlist() -> None:
    argv = start(ClaudeBackend(), freedom="publish")
    index = argv.index("--allowedTools")
    del argv[index : index + 2]
    with pytest.raises(ClaudeUnsafe, match="allowedTools"):
        ClaudeBackend().assert_safe(argv, "publish")


def test_claude_assert_safe_rejects_deny_patterns_present_at_publish() -> None:
    argv = with_extra_options(
        start(ClaudeBackend(), freedom="publish"), "--disallowedTools", DISALLOWED_TOOLS
    )
    with pytest.raises(ClaudeUnsafe, match="disallowedTools"):
        ClaudeBackend().assert_safe(argv, "publish")


def test_claude_assert_safe_rejects_missing_deny_patterns_at_write_in_repo() -> None:
    argv = start(ClaudeBackend(), freedom="write_in_repo")
    index = argv.index("--disallowedTools")
    del argv[index : index + 2]
    with pytest.raises(ClaudeUnsafe, match="disallowedTools"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


# Three evasions measured directly against the real `claude` CLI — each honoured by claude while
# the pre-strict-walk `assert_safe` (search/count-based) let it through. Every one is a single
# extra option appended to an otherwise-legitimate start argv.
CLAUDE_EVASIONS: dict[str, tuple[str, ...]] = {
    # Defeats the check that --permission-mode matches the requested freedom.
    "attached --permission-mode=": ("--permission-mode=bypassPermissions",),
    # A full permission bypass, and was not even in FORBIDDEN_FLAGS.
    "--dangerously-skip-permissions": ("--dangerously-skip-permissions",),
    # claude's own alias for --disallowedTools, attached form: lets a deny list coexist with the
    # allow list at `publish`, and deny beats allow.
    "attached --disallowed-tools= alias": (f"--disallowed-tools={DISALLOWED_TOOLS}",),
}


@pytest.mark.parametrize("evasion", CLAUDE_EVASIONS, ids=list(CLAUDE_EVASIONS))
def test_claude_refuses_every_measured_non_canonical_option_form(evasion: str) -> None:
    argv = with_extra_options(start(ClaudeBackend()), *CLAUDE_EVASIONS[evasion])
    with pytest.raises(ClaudeUnsafe):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


CLAUDE_ATTACHED_FLAG_FORMS = [
    f"{flag}=x"
    for flag in (
        "--output-format", "--permission-mode", "--disallowedTools", "--allowedTools",
        "--max-turns", "--model", "--effort", "--session-id", "--resume",
    )
]


@pytest.mark.parametrize("token", CLAUDE_ATTACHED_FLAG_FORMS)
def test_claude_refuses_the_attached_form_of_every_flag_it_writes(token: str) -> None:
    """`--flag=value` is honoured by claude but never written by this backend, so it must be
    refused like any other non-canonical spelling — not just the three flags actually exploited."""
    argv = with_extra_options(start(ClaudeBackend()), token)
    with pytest.raises(ClaudeUnsafe, match="unrecognised option token"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("alias", ["--allowed-tools", "--disallowed-tools"])
def test_claude_refuses_documented_tool_flag_aliases(alias: str) -> None:
    """claude's own --help documents these as aliases of --allowedTools/--disallowedTools; this
    backend never writes them, so they must be refused like any other unrecognised token."""
    argv = with_extra_options(start(ClaudeBackend()), alias, "Bash(git commit:*)")
    with pytest.raises(ClaudeUnsafe, match="unrecognised option token"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_refuses_an_unrecognised_token() -> None:
    argv = with_extra_options(start(ClaudeBackend()), "--frobnicate")
    with pytest.raises(ClaudeUnsafe, match="unrecognised option token"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("freedom", ["read_only", "write_in_repo"])
def test_claude_deny_and_allow_can_never_coexist_whatever_the_spelling(freedom: str) -> None:
    """Structural, not incidental: forcing an --allowedTools onto a deny freedom must be refused
    even though today's DENY_FREEDOMS/ALLOWED_TOOLS mapping never asks for both at once."""
    argv = with_extra_options(
        start(ClaudeBackend(), freedom=freedom), "--allowedTools", ALLOWED_TOOLS["publish"]
    )
    with pytest.raises(ClaudeUnsafe, match="both present"):
        ClaudeBackend().assert_safe(argv, freedom)


def test_claude_deny_and_allow_can_never_coexist_at_publish() -> None:
    argv = with_extra_options(
        start(ClaudeBackend(), freedom="publish"), "--disallowedTools", DISALLOWED_TOOLS
    )
    with pytest.raises(ClaudeUnsafe, match="both present"):
        ClaudeBackend().assert_safe(argv, "publish")


@pytest.mark.parametrize("model", ["--effort=high", "-sother", "--unknown-flag"])
def test_claude_refuses_a_model_that_would_smuggle_in_an_option(model: str) -> None:
    """`model` is caller-supplied and lands in the option region, so its shape is not trusted."""
    with pytest.raises(ClaudeUnsafe, match="parse as an option"):
        ClaudeBackend().build_start_argv(
            "do a thing", repo=REPO, freedom="write_in_repo", session_id=SESSION, model=model,
            max_turns=None, reasoning_effort=None,
        )


def test_claude_refuses_a_session_id_that_would_smuggle_in_an_option() -> None:
    with pytest.raises(ClaudeUnsafe, match="parse as an option"):
        ClaudeBackend().build_start_argv(
            "do a thing", repo=REPO, freedom="write_in_repo", session_id="--verbose", model=None,
            max_turns=None, reasoning_effort=None,
        )


def test_claude_refuses_a_model_of_bare_separator() -> None:
    """A caller-supplied `model` equal to `--` cannot become a false end-of-options marker.

    `argv.index("--")` finds this fake one first, before the real separator the builder appends —
    so the option region gets truncated right after `--model` (no value pairs with it), and the
    "positional" region balloons to include the real `--`, the prompt, and everything the truncation
    dropped. Confirmed (Codex round-2 review): this fails loudly on positional arity rather than
    silently treating anything as an option — never a bypass, just a different, still-safe message
    than a mid-region smuggle attempt.
    """
    with pytest.raises(ClaudeUnsafe, match="expected exactly one positional"):
        ClaudeBackend().build_start_argv(
            "do a thing", repo=REPO, freedom="write_in_repo", session_id=SESSION, model="--",
            max_turns=None, reasoning_effort=None,
        )


def test_claude_refuses_a_session_id_of_bare_separator() -> None:
    """Same reasoning as test_claude_refuses_a_model_of_bare_separator, one flag later."""
    with pytest.raises(ClaudeUnsafe, match="expected exactly one positional"):
        ClaudeBackend().build_start_argv(
            "do a thing", repo=REPO, freedom="write_in_repo", session_id="--", model=None,
            max_turns=None, reasoning_effort=None,
        )


def test_claude_refuses_a_session_flag_with_no_session_id() -> None:
    argv = start(ClaudeBackend())
    argv[argv.index("--session-id") + 1] = "  "
    with pytest.raises(ClaudeUnsafe, match="no session id"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_rejects_output_format_other_than_stream_json() -> None:
    argv = start(ClaudeBackend())
    argv[argv.index("--output-format") + 1] = "text"
    with pytest.raises(ClaudeUnsafe, match="stream-json"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_rejects_a_duplicate_output_format_whose_second_value_would_win() -> None:
    argv = with_extra_options(start(ClaudeBackend()), "--output-format", "text")
    with pytest.raises(ClaudeUnsafe, match="--output-format appears"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_rejects_a_duplicate_max_turns_whose_second_value_would_win() -> None:
    argv = with_extra_options(start(ClaudeBackend(), max_turns=3), "--max-turns", "99")
    with pytest.raises(ClaudeUnsafe, match="--max-turns appears"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


def test_claude_rejects_a_duplicate_model_whose_second_value_would_win() -> None:
    argv = with_extra_options(start(ClaudeBackend(), model="opus"), "--model", "sonnet")
    with pytest.raises(ClaudeUnsafe, match="--model appears"):
        ClaudeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("build", [start, resume], ids=["start", "resume"])
def test_claude_legitimate_argv_with_model_and_effort_still_passes_the_strict_walk(build) -> None:
    """The allowlist must not be so strict it refuses what this backend itself writes, on either
    the start or resume argv shape."""
    argv = build(ClaudeBackend(), model="sonnet", reasoning_effort="medium")
    ClaudeBackend().assert_safe(argv, "write_in_repo")


# --- codex specifics -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("freedom", "mode"),
    [("read_only", "read-only"), ("write_in_repo", "workspace-write"),
     ("publish", "workspace-write"), ("unrestricted", "danger-full-access")],
)
def test_codex_freedom_maps_to_a_sandbox_mode(freedom: str, mode: str) -> None:
    argv = start(CodexBackend(), freedom=freedom)
    assert argv[argv.index("-s") + 1] == mode


def test_codex_publish_carries_the_network_pair_on_top_of_workspace_write() -> None:
    argv = start(CodexBackend(), freedom="publish")
    assert argv[argv.index("-s") + 1] == "workspace-write"
    assert NETWORK_ENABLE_PAIR in argv
    CodexBackend().assert_safe(argv, "publish")


@pytest.mark.parametrize("freedom", ["read_only", "write_in_repo", "unrestricted"])
def test_codex_network_pair_absent_outside_publish(freedom: str) -> None:
    assert NETWORK_ENABLE_PAIR not in start(CodexBackend(), freedom=freedom)


def test_codex_assert_safe_refuses_the_network_pair_outside_publish() -> None:
    argv = with_extra_options(start(CodexBackend(), freedom="write_in_repo"), "-c", NETWORK_ENABLE_PAIR)
    with pytest.raises(CodexUnsafe, match="network"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_assert_safe_refuses_publish_missing_the_network_pair() -> None:
    argv = start(CodexBackend(), freedom="publish")
    index = argv.index(NETWORK_ENABLE_PAIR)
    del argv[index - 1 : index + 1]  # the "-c" token and the pair value
    assert NETWORK_ENABLE_PAIR not in argv
    with pytest.raises(CodexUnsafe, match="network"):
        CodexBackend().assert_safe(argv, "publish")


def test_codex_asserts_network_blocked_rather_than_hoping_the_user_config_agrees() -> None:
    """`network_access: blocked` has to be true of the run, not of whoever's machine it is on.

    Measured: with `[sandbox_workspace_write] network_access = true` in the user's own codex config,
    `-s workspace-write` with no `-c` pair reached the network (HTTP 200). The explicit `=false`
    overrides that (measured: back to curl exit 6), so the claim holds by construction.
    """
    argv = start(CodexBackend(), freedom="write_in_repo")

    assert NETWORK_DISABLE_PAIR in argv
    assert CodexBackend().enforcement("write_in_repo").network_access == "blocked"

    index = argv.index(NETWORK_DISABLE_PAIR)
    del argv[index - 1 : index + 1]
    with pytest.raises(CodexUnsafe, match="network"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_read_only_takes_no_network_override_because_the_key_does_not_apply() -> None:
    """Measured: `read-only` stayed blocked even with the user config setting the key true, so the
    key is genuinely scoped to `workspace-write` and asserting it here would be noise."""
    argv = start(CodexBackend(), freedom="read_only")

    assert not [a for a in argv if "network_access" in a]
    assert CodexBackend().enforcement("read_only").network_access == "blocked"


def test_codex_refuses_both_network_overrides_at_once() -> None:
    """Which one wins would then depend on argument order rather than the requested freedom."""
    argv = with_extra_options(start(CodexBackend(), freedom="publish"), "-c", NETWORK_DISABLE_PAIR)

    with pytest.raises(CodexUnsafe, match="both network overrides"):
        CodexBackend().assert_safe(argv, "publish")


def test_codex_pins_never_ask_or_it_could_hang() -> None:
    """A headless run that stops for approval waits forever, so this is not optional."""
    argv = start(CodexBackend())
    assert NEVER_ASK[1] in argv
    # Drop the whole `-c approval_policy="never"` pair, not just its value: leaving a dangling
    # `-c` right before `--` would be a missing-value argv the strict walker rejects for that
    # reason, which is a different failure than the missing-override one this test means to cover.
    # Delete it positionally rather than filtering every `-c` out, which would also orphan the
    # network override's own `-c` and fail on that instead.
    index = argv.index(NEVER_ASK[1])
    del argv[index - 1 : index + 1]
    with pytest.raises(CodexUnsafe, match="approval"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_rejects_a_later_approval_override_that_would_win() -> None:
    """Old behaviour: refused only because a second approval_policy override would win. Now
    refused more directly — `on-request` is not `PERMITTED_C_PAIRS`' literal at all, so any `-c`
    override besides the exact one this backend writes is rejected outright."""
    argv = with_extra_options(start(CodexBackend()), "-c", 'approval_policy="on-request"')
    with pytest.raises(CodexUnsafe, match="unexpected -c pair"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_rejects_a_duplicate_sandbox_under_its_long_alias() -> None:
    """`-s` and `--sandbox` are the same option to codex, so a second one would silently win.
    The allowlist now refuses `--sandbox` outright as a non-canonical token — this backend only
    ever writes `-s` — rather than reaching a "two sandbox flags" count."""
    argv = with_extra_options(start(CodexBackend()), "--sandbox", "danger-full-access")
    with pytest.raises(CodexUnsafe, match="unrecognised option token"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_separates_the_prompt_so_it_cannot_be_parsed_as_options() -> None:
    argv = CodexBackend().build_start_argv(
        "--sandbox", repo=REPO, freedom="read_only", session_id=None, model=None, max_turns=None,
        reasoning_effort=None,
    )
    assert argv[-2:] == ["--", "--sandbox"]
    # And prompt text must not be able to satisfy a safety check.
    with pytest.raises(CodexUnsafe, match="exactly one sandbox"):
        CodexBackend().assert_safe(
            [a for a in argv if a not in ("-s", "read-only")], "read_only"
        )


def test_codex_refuses_a_turn_cap_rather_than_dropping_it() -> None:
    """Silently ignoring it would leave a direct caller believing a limit applied."""
    with pytest.raises(backends.UnsupportedCapability, match="no turn cap"):
        start(CodexBackend(), max_turns=5)


def test_a_top_level_codex_error_event_is_a_failure() -> None:
    """Unlike an `error` *item*, which was observed in a successful run."""
    acc = ingest(CodexBackend(), [*CODEX_EVENTS, {"type": "error", "message": "boom"}])

    assert acc.is_error is True
    assert CodexBackend().classify(acc, 0) == "failed"


def test_codex_refuses_to_discard_its_sandbox() -> None:
    argv = with_extra_options(start(CodexBackend()), "--dangerously-bypass-approvals-and-sandbox")
    with pytest.raises(CodexUnsafe, match="discards the sandbox"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_prompt_mentioning_the_bypass_flag_is_not_treated_as_using_it() -> None:
    """Pre-existing bug, fixed: the check used to search the whole argv, including prompt text
    after `--`, contradicting its own comment that only the option region is meant to be trusted."""
    argv = CodexBackend().build_start_argv(
        "please --dangerously-bypass-approvals-and-sandbox everything",
        repo=REPO, freedom="write_in_repo", session_id=None, model=None, max_turns=None,
        reasoning_effort=None,
    )
    CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_prompt_is_last_so_no_option_swallows_it() -> None:
    assert start(CodexBackend())[-1] == "do a thing"
    # `codex exec resume [SESSION_ID] [PROMPT]` — both positional, in that order.
    assert resume(CodexBackend())[-3:] == ["--", "abc-123", "more"]


def test_codex_resume_argv_puts_the_option_region_before_resume() -> None:
    """The bug this backend shipped with: `-C`/`-s` are global `codex exec` options, absent from
    `codex exec resume --help` (measured on codex-cli 0.154.0) — so they must sit ahead of `resume`,
    which becomes the region's own last token, immediately before `--`."""
    argv = resume(CodexBackend())
    sep = argv.index("--")
    assert argv[2] != "resume", "options must lead, not `resume`"
    assert argv[sep - 1] == "resume", "`resume` must be the last token before `--`"
    assert argv[sep + 1 :] == ["abc-123", "more"]


def test_codex_start_and_resume_share_a_byte_identical_option_region() -> None:
    """The precise claim, not just "the same options appear somewhere": start's `argv[2:sep]` is
    resume's `argv[2:sep-1]` (i.e. the resume region minus its own trailing `resume` token) —
    `_options` is untouched by the fix, so both argvs must carry it identically."""
    start_argv = start(CodexBackend(), model="gpt-5", reasoning_effort="medium")
    resume_argv = resume(CodexBackend(), model="gpt-5", reasoning_effort="medium")
    start_sep = start_argv.index("--")
    resume_sep = resume_argv.index("--")

    assert resume_argv[resume_sep - 1] == "resume"
    assert start_argv[2:start_sep] == resume_argv[2 : resume_sep - 1]


def test_codex_refuses_the_old_pre_fix_resume_shape() -> None:
    """This is the actual reported bug: `resume` right after `exec`, options after it — codex
    itself rejects it (`error: unexpected argument '-C' found`), so it must never be built or
    waved through again."""
    argv = [
        CODEX_BINARY, "exec", "resume", "--json", "-C", str(REPO), "-s", "read-only",
        "-c", NEVER_ASK[1], "--", "abc-123", "more",
    ]
    with pytest.raises(CodexUnsafe, match="last token"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_refuses_a_duplicated_resume_token() -> None:
    """Two real `resume` tokens (not one consumed as a flag's value): the first can never be the
    region's last token, so it is refused there — same check as the non-terminal case, since a
    duplicate always makes an earlier occurrence non-final."""
    argv = resume(CodexBackend())
    sep = argv.index("--")
    assert argv[sep - 1] == "resume"
    argv = [*argv[: sep - 1], "resume", *argv[sep - 1 :]]
    with pytest.raises(CodexUnsafe, match="last token"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_model_named_resume_is_not_read_as_the_subcommand_on_start() -> None:
    """`-m resume`'s value is consumed in value position, never examined as a candidate subcommand
    token — so this stays a 1-positional start argv, not a 2-positional resume one."""
    argv = start(CodexBackend(), model="resume")
    assert argv[argv.index("-m") + 1] == "resume"
    CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_model_named_resume_still_passes_on_an_actual_resume() -> None:
    """`-m resume resume`: the first "resume" is `-m`'s value, and only the second, trailing one is
    the real subcommand token — unambiguous by construction, per `_parse_options`' docstring."""
    argv = resume(CodexBackend(), model="resume")
    sep = argv.index("--")
    assert argv[sep - 2 : sep] == ["resume", "resume"]
    CodexBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize(
    "argv, why",
    [
        (
            ["codex", "exec", "--json", "-C", str(REPO), "-s", "read-only", "-c", NEVER_ASK[1], "--"],
            "a start argv with no prompt at all",
        ),
        (
            [
                "codex", "exec", "--json", "-C", str(REPO), "-s", "read-only",
                "-c", NEVER_ASK[1], "resume", "--", "only-one",
            ],
            "a resume missing its prompt, where codex would read the prompt as the session id",
        ),
        (
            [
                "codex", "exec", "--json", "--json", "-C", str(REPO), "-s", "read-only",
                "-c", NEVER_ASK[1], "--", "p",
            ],
            "a duplicated --json this backend never writes",
        ),
        (
            ["codex", "exec", "--json", "-C", "", "-s", "read-only", "-c", NEVER_ASK[1], "--", "p"],
            "an empty -C, which names no directory",
        ),
        (
            [
                "codex", "exec", "--json", "-C", str(REPO), "-s", "read-only",
                "-c", NEVER_ASK[1], "-m", "", "--", "p",
            ],
            "an empty -m, which _options would have omitted entirely",
        ),
    ],
)
def test_codex_refuses_shapes_it_would_never_have_written(argv: list[str], why: str) -> None:
    """The allowlist promises only canonical tokens; arity and emptiness are part of that claim.

    The resume case is the one with teeth: one positional instead of two means codex takes the
    intended prompt as SESSION_ID and silently continues a different conversation.
    """
    with pytest.raises(CodexUnsafe):
        CodexBackend().assert_safe(argv, "read_only")


def test_codex_positional_arity_allows_values_that_look_like_options() -> None:
    """Arity is counted, never matched: a real prompt or session id may look like a flag."""
    backend = CodexBackend()
    backend.assert_safe(
        backend.build_resume_argv(
            "--json", repo=REPO, freedom="read_only", session_id="resume",
            model=None, max_turns=None, reasoning_effort="xhigh",
        ),
        "read_only",
    )


def test_codex_positionals_may_be_the_separator_or_the_subcommand_itself() -> None:
    """`_parse_options`' docstring promises positionals are *counted*, never matched — so the two
    strings that carry structural meaning in the option region, `--` and `resume`, must still be
    deliverable as a session id and a prompt. Everything after the first `--` is positional, so
    neither can reach the walker.
    """
    backend = CodexBackend()
    argv = backend.build_resume_argv(
        "resume", repo=REPO, freedom="read_only", session_id="--",
        model=None, max_turns=None, reasoning_effort=None,
    )
    assert argv[-3:] == ["--", "--", "resume"]
    backend.assert_safe(argv, "read_only")


def test_codex_working_directory_is_explicit() -> None:
    """Codex takes its root as a flag rather than inheriting the spawn cwd."""
    argv = start(CodexBackend())
    assert argv[argv.index("-C") + 1] == str(REPO)


def test_codex_resume_needs_a_session_id() -> None:
    with pytest.raises(ValueError, match="thread id"):
        resume(CodexBackend(), session_id="")


def test_codex_emits_exactly_one_effort_pair_and_still_pins_approval_policy() -> None:
    argv = start(CodexBackend(), reasoning_effort="high")
    assert argv.count(f'{CODEX_EFFORT_KEY}="high"') == 1
    assert NEVER_ASK[1] in argv
    CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_rejects_a_duplicate_effort_override_whose_second_value_would_win() -> None:
    argv = with_extra_options(
        start(CodexBackend(), reasoning_effort="low"), "-c", f'{CODEX_EFFORT_KEY}="high"'
    )
    with pytest.raises(CodexUnsafe, match="at most one"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_rejects_an_unexpected_effort_value_reaching_assert_safe_directly() -> None:
    argv = with_extra_options(start(CodexBackend()), "-c", f'{CODEX_EFFORT_KEY}="bogus"')
    with pytest.raises(CodexUnsafe, match="unexpected"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_a_look_alike_key_does_not_satisfy_the_approval_policy_requirement() -> None:
    """Old bug: value.startswith("approval_policy") also matched approval_policy_extra=…, so a
    look-alike key could stand in for — rather than being rejected in place of — the real
    override. Now refused even earlier, at the exact-literal check: `approval_policy_extra="never"`
    is not one of the two pairs this backend ever writes, regardless of what it shares a prefix
    with."""
    argv = [
        CODEX_BINARY, "exec", "--json", "-C", str(REPO), "-s", "workspace-write",
        "-c", 'approval_policy_extra="never"',
        "--", "do a thing",
    ]
    with pytest.raises(CodexUnsafe, match="unexpected -c pair"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_an_unrelated_key_sharing_a_prefix_is_still_refused() -> None:
    """Was `..._does_not_false_positive`, and used to assert this passed: a harmless-looking extra
    `-c` key must not make the *real* override look like a violation just because it shares a
    prefix. The allowlist closes the hole a different way — every `-c` pair not in
    PERMITTED_C_PAIRS is refused outright, "harmless" or not, so there is no longer a passing case
    to keep here; strict allowlisting subsumes the narrower prefix-matching fix."""
    argv = with_extra_options(start(CodexBackend()), "-c", 'approval_policy_extra="never"')
    with pytest.raises(CodexUnsafe, match="unexpected -c pair"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_rejects_a_model_value_that_looks_like_an_option() -> None:
    argv = with_extra_options(start(CodexBackend()), "-m", "--something")
    with pytest.raises(CodexUnsafe, match="parse as an option"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_refuses_a_space_padded_approval_policy_override() -> None:
    """Measured: codex normalises whitespace around `-c key=value` before applying it — `-c
    'approval_policy ="on-request"'` (space before `=`) was honoured by codex itself. The
    exact-literal allowlist refuses it even more directly than before: that spacing does not match
    the one literal this backend writes, so it never reaches the approval-count check at all."""
    argv = with_extra_options(start(CodexBackend()), "-c", 'approval_policy ="on-request"')
    with pytest.raises(CodexUnsafe, match="unexpected -c pair"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_refuses_a_space_padded_duplicate_effort_override() -> None:
    """As above, for the effort override: the space before `=` means this never matches a
    PERMITTED_C_PAIRS literal, so it is refused before the duplicate-effort count runs."""
    argv = with_extra_options(
        start(CodexBackend(), reasoning_effort="low"), "-c", f'{CODEX_EFFORT_KEY} ="high"'
    )
    with pytest.raises(CodexUnsafe, match="unexpected -c pair"):
        CodexBackend().assert_safe(argv, "write_in_repo")


def test_codex_refuses_a_config_pair_with_no_equals() -> None:
    """Previously dropped from `keyed` and thus ignored entirely, even though codex still applies
    something from it — refused outright instead."""
    argv = with_extra_options(start(CodexBackend()), "-c", "model_reasoning_effort")
    with pytest.raises(CodexUnsafe, match="no '='"):
        CodexBackend().assert_safe(argv, "write_in_repo")


# Eight evasions measured directly against the real `codex` CLI — each honoured by codex while a
# search-based `assert_safe` (the pre-strict-walk version) let it through. Every one is a single
# extra option appended to an otherwise-legitimate start argv.
CODEX_EVASIONS: dict[str, tuple[str, ...]] = {
    "attached --config=": ('--config=approval_policy="on-request"',),
    "attached -c": ('-capproval_policy="on-request"',),
    # Verified live: `codex exec --json --sandbox=read-only …` runs fine, so this attached form
    # is real, not merely plausible from --help.
    "attached --sandbox=": ("--sandbox=danger-full-access",),
    "--add-dir widens the writable set": ("--add-dir", "/etc"),
    # Unbalanced quote: codex falls back to the raw string for a value that fails TOML parsing,
    # so this would reach codex as the literal (invalid) string rather than being rejected there.
    "unbalanced quote": ("-c", 'model_reasoning_effort="low'),
    "doubled quoting": ("-c", 'model_reasoning_effort=""low""'),
    # Both attached model spellings were measured honoured on codex-cli 0.154.0: each sent the
    # value to the API as the model (`The 'resume' model is not supported …`), so neither is a
    # token codex ignores. Neither is a token this backend's walker recognises either — whatever
    # value rides on it, including the one ("resume") that a check searching for the subcommand
    # rather than walking to it could mistake for one.
    "attached --model=resume": ("--model=resume",),
    "attached -mresume": ("-mresume",),
}


@pytest.mark.parametrize("evasion", CODEX_EVASIONS, ids=list(CODEX_EVASIONS))
def test_codex_refuses_every_measured_non_canonical_option_form(evasion: str) -> None:
    argv = with_extra_options(start(CodexBackend()), *CODEX_EVASIONS[evasion])
    with pytest.raises(CodexUnsafe):
        CodexBackend().assert_safe(argv, "write_in_repo")


def with_extra_options_before_codex_resume(argv: list[str], *extra: str) -> list[str]:
    """Like `with_extra_options`, but inserts before the trailing `resume` token rather than
    before `--`.

    On a resume argv those are no longer the same position: `resume` itself now sits immediately
    before `--`. The shared helper would insert an evasion *after* the subcommand, which is a
    start-shaped injection into a resume argv — codex would probably still refuse it, but not for
    the reason this test claims to cover, and a bug that only manifested when the evasion landed
    ahead of `resume` would pass silently.
    """
    cut = argv.index("--") - 1
    assert argv[cut] == "resume", f"expected a trailing resume token right before '--': {argv!r}"
    return [*argv[:cut], *extra, *argv[cut:]]


@pytest.mark.parametrize("evasion", CODEX_EVASIONS, ids=list(CODEX_EVASIONS))
def test_codex_refuses_every_measured_non_canonical_option_form_on_resume(evasion: str) -> None:
    argv = with_extra_options_before_codex_resume(resume(CodexBackend()), *CODEX_EVASIONS[evasion])
    with pytest.raises(CodexUnsafe):
        CodexBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("build", [start, resume], ids=["start", "resume"])
def test_codex_legitimate_argv_with_model_and_effort_still_passes_the_strict_walk(build) -> None:
    """The allowlist must not be so strict it refuses what this backend itself writes. `start` and
    `resume` no longer differ in where the option region *begins* — both start right after `exec` —
    only in whether a trailing `resume` token follows it before `--`."""
    argv = build(CodexBackend(), model="gpt-5", reasoning_effort="medium")
    CodexBackend().assert_safe(argv, "write_in_repo")


# --- opencode specifics --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("freedom", "agent", "auto"),
    [("read_only", "plan", False), ("write_in_repo", "build", False),
     ("publish", "build", False), ("unrestricted", "build", True)],
)
def test_opencode_freedom_maps_to_an_agent(freedom: str, agent: str, auto: bool) -> None:
    argv = start(OpencodeBackend(), freedom=freedom)

    assert argv[argv.index("--agent") + 1] == agent
    assert ("--auto" in argv) is auto


def test_opencode_read_only_never_carries_auto_approval() -> None:
    argv = with_extra_options(start(OpencodeBackend(), freedom="read_only"), "--auto")
    with pytest.raises(OpencodeUnsafe, match="contradicts"):
        OpencodeBackend().assert_safe(argv, "read_only")


@pytest.mark.parametrize("flag", REJECTED_FLAGS)
def test_opencode_refuses_flags_that_break_its_session_or_permission_guarantees(flag: str) -> None:
    """-c races on "most recent", --fork mints a new id, --attach runs it somewhere else."""
    argv = start(OpencodeBackend())
    assert flag not in argv
    with pytest.raises(OpencodeUnsafe, match="guarantees"):
        OpencodeBackend().assert_safe(with_extra_options(argv, flag), "write_in_repo")


def test_opencode_rejects_a_non_json_format_it_could_not_parse() -> None:
    argv = start(OpencodeBackend())
    argv[argv.index("--format") + 1] = "default"
    with pytest.raises(OpencodeUnsafe, match="only json"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("flag", ["--format", "--dir", "--agent"])
def test_opencode_rejects_a_duplicate_option_whose_second_value_would_win(flag: str) -> None:
    argv = with_extra_options(start(OpencodeBackend()), flag, "whatever")
    with pytest.raises(OpencodeUnsafe, match="appears 2 times"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize(
    "token",
    [
        "--format=default", "--agent=build", "--session=abc", "-sabc", "-mgpt",
        "--dir=/elsewhere", "--variant=high",
    ],
)
def test_opencode_refuses_option_forms_that_would_override_a_guarantee_unnoticed(
    token: str,
) -> None:
    """Measured: appending `--format=default` to an argv that already had `--format json` disabled
    the JSON stream — the later value wins, and a search-based check never saw it."""
    argv = with_extra_options(start(OpencodeBackend()), token)

    with pytest.raises(OpencodeUnsafe, match="unrecognised option token"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_refuses_a_second_model_that_would_win() -> None:
    """The run would use a model other than the one the task reports."""
    argv = with_extra_options(start(OpencodeBackend(), model="a/b"), "-m", "other/model")

    with pytest.raises(OpencodeUnsafe, match="-m appears 2 times"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_accepts_variant_in_canonical_form() -> None:
    argv = start(OpencodeBackend(), reasoning_effort="high")

    assert argv[argv.index("--variant") + 1] == "high"
    OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_rejects_a_duplicate_variant_whose_second_value_would_win() -> None:
    argv = with_extra_options(start(OpencodeBackend(), reasoning_effort="low"), "--variant", "high")

    with pytest.raises(OpencodeUnsafe, match="appears 2 times"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_rejects_an_unexpected_variant_value_reaching_assert_safe_directly() -> None:
    argv = with_extra_options(start(OpencodeBackend()), "--variant", "bogus")

    with pytest.raises(OpencodeUnsafe, match="unexpected --variant"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_start_carries_no_session_flag_and_resume_carries_one() -> None:
    started, resumed = start(OpencodeBackend()), resume(OpencodeBackend())

    assert "-s" not in started[: started.index("--")]
    assert resumed[resumed.index("-s") + 1] == "abc-123"
    # -c would continue whatever ran last on this machine, which is not necessarily this session.
    assert "-c" not in resumed


def test_opencode_refuses_a_session_flag_with_no_session_id() -> None:
    argv = with_extra_options(start(OpencodeBackend()), "-s", "  ")
    with pytest.raises(OpencodeUnsafe, match="no session id"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_resume_needs_a_session_id() -> None:
    with pytest.raises(ValueError, match="session id"):
        resume(OpencodeBackend(), session_id="")


def test_an_opencode_prompt_that_looks_like_a_flag_is_not_mistaken_for_one() -> None:
    """Measured: `-- "--auto …"` is passed through as text and changes nothing."""
    argv = OpencodeBackend().build_start_argv(
        "--auto --format default", repo=REPO, freedom="read_only", session_id=None, model=None,
        max_turns=None, reasoning_effort=None,
    )

    assert argv[-2:] == ["--", "--auto --format default"]
    OpencodeBackend().assert_safe(argv, "read_only")


def test_opencode_working_directory_is_explicit() -> None:
    """--dir is what puts the repo on the command line for tasks._identity_markers."""
    argv = start(OpencodeBackend())

    assert argv[argv.index("--dir") + 1] == str(REPO)


def test_opencode_refuses_a_turn_cap_rather_than_dropping_it() -> None:
    with pytest.raises(backends.UnsupportedCapability, match="no turn cap"):
        start(OpencodeBackend(), max_turns=5)


def test_opencode_claims_no_more_about_auto_approval_than_was_measured() -> None:
    """One run showed `build` writing without --auto. That does not prove --auto never matters, so
    the caveat must not say it does."""
    caveats = " ".join(OpencodeBackend().enforcement("unrestricted").caveats)

    assert "not what separates writing from not writing" in caveats
    assert "depends entirely on that configuration" in caveats


@pytest.mark.parametrize("model", ["--continue=true", "--agent=build", "-sother"])
def test_opencode_refuses_a_model_that_would_smuggle_in_an_option(model: str) -> None:
    """`model` is caller-supplied and lands in the option region, so its shape is not trusted."""
    with pytest.raises(OpencodeUnsafe, match="parse as an option"):
        start(OpencodeBackend(), model=model)


def test_opencode_refuses_a_flag_that_ate_the_next_flag_as_its_value() -> None:
    argv = start(OpencodeBackend())
    argv[argv.index("--dir") + 1] = "--agent"

    with pytest.raises(OpencodeUnsafe, match="parse as an option"):
        OpencodeBackend().assert_safe(argv, "write_in_repo")


def test_opencode_read_only_is_described_as_restraint_not_prevention() -> None:
    """The plan run never attempted a write, so the caveat must not claim a tool layer would have
    let one through — only that nothing exercised it."""
    enforcement = OpencodeBackend().enforcement("read_only")

    assert enforcement.os_enforced is False
    caveats = " ".join(enforcement.caveats)
    assert "declines" in caveats
    assert "was not exercised" in caveats


# --- vibe specifics ------------------------------------------------------------------------

# vibe has no `--` separator (the PROMPT positional is ignored in programmatic mode — see the
# module docstring), so the prompt rides on a single trailing `--prompt=<text>` token instead.
# `with_extra_options` inserts before `--`, which does not exist here and would silently append
# after the prompt; this inserts before that trailing token instead.
def with_extra_vibe_options(argv: list[str], *extra: str) -> list[str]:
    return [*argv[:-1], *extra, argv[-1]]


@pytest.mark.parametrize(
    ("freedom", "agent"),
    [("read_only", "plan"), ("write_in_repo", "accept-edits"), ("publish", "auto-approve"),
     ("unrestricted", "auto-approve")],
)
def test_vibe_freedom_maps_to_an_agent(freedom: str, agent: str) -> None:
    argv = start(VibeBackend(), freedom=freedom)
    assert argv[argv.index("--agent") + 1] == agent
    assert VIBE_AGENTS[freedom] == agent


def test_vibe_working_directory_is_explicit() -> None:
    """--workdir is what puts the repo on the command line for tasks._identity_markers."""
    argv = start(VibeBackend())
    assert argv[argv.index("--workdir") + 1] == str(REPO)


def test_vibe_turn_cap_is_emitted() -> None:
    """Unlike codex and opencode, vibe genuinely supports a turn cap — see its capabilities."""
    argv = start(VibeBackend(), max_turns=7)
    assert argv[argv.index("--max-turns") + 1] == "7"
    VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_resume_needs_a_session_id() -> None:
    with pytest.raises(ValueError, match="session id"):
        resume(VibeBackend(), session_id="")


def test_a_vibe_prompt_that_looks_like_a_flag_is_not_mistaken_for_one() -> None:
    """Measured: a prompt beginning `--max-turns` reached the model verbatim at exit 0."""
    argv = VibeBackend().build_start_argv(
        "--max-turns 999 --agent auto-approve", repo=REPO, freedom="read_only", session_id=None,
        model=None, max_turns=None, reasoning_effort=None,
    )
    assert argv[-1] == "--prompt=--max-turns 999 --agent auto-approve"
    VibeBackend().assert_safe(argv, "read_only")


def test_vibe_prompt_must_be_the_last_token() -> None:
    """The PROMPT positional is ignored in programmatic mode, so anything after --prompt=... would
    never reach the model — refused rather than silently dropped."""
    argv = [*start(VibeBackend()), "--trust"]
    with pytest.raises(VibeUnsafe, match="last token"):
        VibeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize(
    "token", ["--prompt=", "--prompt=   ", "--prompt=\n", "--prompt=\t "],
    ids=["empty", "spaces", "newline", "tab"],
)
def test_vibe_refuses_an_empty_or_blank_prompt_value(token: str) -> None:
    """`--prompt=` with no real text passes a bare `startswith` check, and whitespace passes a bare
    truthiness check; assert_safe is the final execution seam (re-run at spawn time in tasks.py), so
    it must require a genuinely non-empty prompt itself — matching `_check_prompt`, which strips."""
    argv = start(VibeBackend())
    argv[-1] = token
    with pytest.raises(VibeUnsafe, match="non-empty"):
        VibeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("flag", VIBE_REJECTED_FLAGS)
def test_vibe_refuses_flags_that_break_its_guarantees(flag: str) -> None:
    argv = start(VibeBackend())
    assert flag not in argv
    with pytest.raises(VibeUnsafe, match="guarantees"):
        VibeBackend().assert_safe(with_extra_vibe_options(argv, flag), "write_in_repo")


@pytest.mark.parametrize(
    "token",
    ["--output=streaming", "--agent=plan", "--workdir=/elsewhere", "--resume=abc"],
)
def test_vibe_refuses_option_forms_that_would_override_a_guarantee_unnoticed(token: str) -> None:
    argv = with_extra_vibe_options(start(VibeBackend()), token)
    with pytest.raises(VibeUnsafe, match="unrecognised option token"):
        VibeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize(
    ("flag", "extra", "why"),
    [("--output", ("whatever",), "appears"), ("--agent", ("whatever",), "appears"),
     ("--trust", (), "exactly one --trust")],
)
def test_vibe_rejects_a_duplicate_boolean_or_value_flag_whose_second_value_would_win(
    flag: str, extra: tuple[str, ...], why: str,
) -> None:
    argv = with_extra_vibe_options(start(VibeBackend()), flag, *extra)
    with pytest.raises(VibeUnsafe, match=why):
        VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_rejects_a_duplicate_max_turns_whose_second_value_would_win() -> None:
    argv = with_extra_vibe_options(start(VibeBackend(), max_turns=3), "--max-turns", "9")
    with pytest.raises(VibeUnsafe, match="--max-turns appears"):
        VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_rejects_a_duplicate_resume_whose_second_value_would_win() -> None:
    argv = with_extra_vibe_options(resume(VibeBackend()), "--resume", "other-session")
    with pytest.raises(VibeUnsafe, match="--resume appears"):
        VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_rejects_a_missing_trust() -> None:
    argv = start(VibeBackend())
    argv.remove("--trust")
    with pytest.raises(VibeUnsafe, match="--trust"):
        VibeBackend().assert_safe(argv, "write_in_repo")


@pytest.mark.parametrize("value", ["0", "-1", "abc", "1.5", ""])
def test_vibe_rejects_a_non_canonical_max_turns_value(value: str) -> None:
    argv = start(VibeBackend(), max_turns=1)
    argv[argv.index("--max-turns") + 1] = value
    with pytest.raises(VibeUnsafe):
        VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_refuses_a_resume_flag_naming_no_session() -> None:
    """assert_safe sees only argv, with no way to tell whether the call it is guarding was meant
    to be a start or a resume — there is no subcommand token here the way `codex exec resume` has
    one (see the plan's Open Questions on the analogous --agent/freedom gap). So the well-formedness
    it can and does police is: at most one --resume, and never one naming no session at all."""
    argv = with_extra_vibe_options(start(VibeBackend()), "--resume", "  ")
    with pytest.raises(VibeUnsafe, match="names no session"):
        VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_start_carries_no_resume_and_resume_carries_one() -> None:
    started, resumed = start(VibeBackend()), resume(VibeBackend())
    assert "--resume" not in started
    assert resumed[resumed.index("--resume") + 1] == "abc-123"


def test_vibe_refuses_a_workdir_value_that_looks_like_an_option() -> None:
    argv = start(VibeBackend())
    argv[argv.index("--workdir") + 1] = "--agent"
    with pytest.raises(VibeUnsafe, match="parse as an option"):
        VibeBackend().assert_safe(argv, "write_in_repo")


def test_vibe_plan_caveat_states_that_bash_is_still_governed_by_the_users_config() -> None:
    caveats = " ".join(VibeBackend().enforcement("read_only").caveats)
    assert "bash" in caveats.lower()


def test_vibe_reasoning_effort_caveat_explains_why_it_is_config_only() -> None:
    caveats = " ".join(VibeBackend().capabilities.reasoning_effort.caveats)
    assert "config-only" in caveats or "config only" in caveats.lower()


@pytest.mark.parametrize("level", EFFORTS)
def test_vibe_refuses_reasoning_effort_at_every_level_on_start_and_resume(level: str) -> None:
    with pytest.raises(backends.UnsupportedCapability, match="no reasoning effort control"):
        start(VibeBackend(), reasoning_effort=level)
    with pytest.raises(backends.UnsupportedCapability, match="no reasoning effort control"):
        resume(VibeBackend(), reasoning_effort=level)


def test_vibe_refuses_a_model_at_start_and_resume() -> None:
    with pytest.raises(backends.UnsupportedCapability, match="no model selection flag"):
        start(VibeBackend(), model="mistral-medium")
    with pytest.raises(backends.UnsupportedCapability, match="no model selection flag"):
        resume(VibeBackend(), model="mistral-medium")



# --- stream normalisation, from captured events --------------------------------------------

CLAUDE_EVENTS = [
    {"type": "system", "subtype": "init", "session_id": SESSION,
     "tools": ["Bash", "Edit"], "mcp_servers": [{"name": "owlex", "status": "pending"}]},
    {"type": "result", "subtype": "success", "session_id": SESSION, "result": "done it",
     "is_error": False, "num_turns": 3, "total_cost_usd": 0.29,
     "permission_denials": [{"tool_name": "Bash"}]},
]

CODEX_EVENTS = [
    {"type": "thread.started", "thread_id": "019fadb8-a8ae-7fb1-9947-b83697badca7"},
    {"type": "turn.started"},
    {"type": "item.completed", "item": {"id": "item_0", "type": "error",
                                        "message": "Skill descriptions were shortened…"}},
    {"type": "item.completed", "item": {"id": "item_1", "type": "agent_message", "text": "ok"}},
    {"type": "turn.completed", "usage": {"input_tokens": 27992, "output_tokens": 5}},
]


OPENCODE_SESSION = "ses_00b8d2f9affeIu2zpaWJR3voFi"

# A three-step run: write the file, run git status, then answer. Costs and token counts are exactly
# as captured — per step, not cumulative.
OPENCODE_EVENTS = [
    {"type": "step_start", "sessionID": OPENCODE_SESSION,
     "part": {"id": "prt_0", "type": "step-start"}},
    {"type": "tool_use", "sessionID": OPENCODE_SESSION,
     "part": {"id": "prt_1", "tool": "write", "state": {"status": "completed"}}},
    {"type": "step_finish", "sessionID": OPENCODE_SESSION,
     "part": {"reason": "tool-calls", "type": "step-finish", "cost": 0.05833915,
              "tokens": {"total": 40054, "input": 40000, "output": 54,
                         "cache": {"write": 0, "read": 0}}}},
    {"type": "step_finish", "sessionID": OPENCODE_SESSION,
     "part": {"reason": "tool-calls", "type": "step-finish", "cost": 0.0149368,
              "tokens": {"total": 100, "input": 90, "output": 10,
                         "cache": {"write": 0, "read": 39936}}}},
    {"type": "text", "sessionID": OPENCODE_SESSION,
     "part": {"type": "text", "text": "Created `probe.txt`, then ran `git status`."}},
    {"type": "step_finish", "sessionID": OPENCODE_SESSION,
     "part": {"reason": "stop", "type": "step-finish", "cost": 0.0148125,
              "tokens": {"total": 160, "input": 126, "output": 34,
                         "cache": {"write": 0, "read": 39936}}}},
]

OPENCODE_ERROR = {
    "type": "error", "sessionID": "ses_00b8b9ecdffeHKaAAuCciBhwlU",
    "error": {"name": "UnknownError",
              "data": {"message": "Unexpected server error.", "ref": "err_a8c661b2"}},
}


def ingest(backend, events) -> Accumulator:
    acc = Accumulator()
    for event in events:
        backend.ingest(json.loads(json.dumps(event)), acc)
    return acc


def test_claude_normalisation() -> None:
    acc = ingest(ClaudeBackend(), CLAUDE_EVENTS)

    assert acc.session_id == SESSION
    assert acc.summary == "done it"
    assert acc.num_turns == 3
    assert acc.total_cost_usd == pytest.approx(0.29)
    assert acc.denials and acc.mcp_servers
    assert acc.available_tool_count == 2
    assert ClaudeBackend().classify(acc, 0) == "completed"


def test_codex_normalisation() -> None:
    acc = ingest(CodexBackend(), CODEX_EVENTS)

    # Codex calls it thread_id, and only reveals it once the run has started.
    assert acc.session_id == "019fadb8-a8ae-7fb1-9947-b83697badca7"
    assert acc.summary == "ok"
    assert acc.num_turns == 1
    assert acc.usage == {"input_tokens": 27992, "output_tokens": 5}
    assert CodexBackend().classify(acc, 0) == "completed"


def test_codex_reports_no_dollar_cost() -> None:
    """It only emits token counts, so inventing a number would be a lie."""
    acc = ingest(CodexBackend(), CODEX_EVENTS)

    assert acc.total_cost_usd is None
    assert CodexBackend().capabilities.reports_cost_usd is False


def test_a_codex_error_item_is_a_notice_not_a_failure() -> None:
    """Observed in a genuinely successful run: a skills warning arrives as an `error` item."""
    acc = ingest(CodexBackend(), CODEX_EVENTS)

    assert acc.notices and "Skill descriptions" in acc.notices[0]
    assert acc.is_error is not True
    assert CodexBackend().classify(acc, 0) == "completed"


def test_codex_without_a_closing_message_is_a_failure() -> None:
    acc = ingest(CodexBackend(), CODEX_EVENTS[:3])

    assert CodexBackend().classify(acc, 0) == "failed"


def test_codex_nonzero_exit_is_a_failure() -> None:
    acc = ingest(CodexBackend(), CODEX_EVENTS)

    assert CodexBackend().classify(acc, 1) == "failed"


def test_claude_max_turns_exhaustion_is_distinguished() -> None:
    acc = ingest(
        ClaudeBackend(),
        [CLAUDE_EVENTS[0], CLAUDE_EVENTS[1] | {"subtype": "error_max_turns", "is_error": True}],
    )

    assert ClaudeBackend().classify(acc, 0) == "timed_out"


def test_claude_with_no_terminal_event_is_a_failure() -> None:
    acc = ingest(ClaudeBackend(), CLAUDE_EVENTS[:1])

    assert ClaudeBackend().classify(acc, 0) == "failed"


def test_opencode_normalisation() -> None:
    acc = ingest(OpencodeBackend(), OPENCODE_EVENTS)

    # sessionID rides on every event, so it is known from the very first line.
    assert acc.session_id == OPENCODE_SESSION
    assert acc.summary == "Created `probe.txt`, then ran `git status`."
    assert acc.num_turns == 3
    assert OpencodeBackend().classify(acc, 0) == "completed"


def test_opencode_sums_per_step_costs_instead_of_reporting_only_the_last() -> None:
    """Measured: cost is per step. Assigning would report 0.0148 for a run that cost 0.0881."""
    acc = ingest(OpencodeBackend(), OPENCODE_EVENTS)

    assert acc.total_cost_usd == pytest.approx(0.05833915 + 0.0149368 + 0.0148125)
    assert acc.total_cost_usd > 0.0148125


def test_opencode_sums_token_counts_including_the_nested_cache_block() -> None:
    acc = ingest(OpencodeBackend(), OPENCODE_EVENTS)

    assert acc.usage is not None
    assert acc.usage["input"] == 40000 + 90 + 126
    assert acc.usage["output"] == 54 + 10 + 34
    assert acc.usage["cache"]["read"] == 39936 * 2


def test_an_intermediate_opencode_step_is_not_the_end_of_the_run() -> None:
    """`reason: tool-calls` means more is coming; only `stop` ends it."""
    up_to_second_step = OPENCODE_EVENTS[:4]
    acc = ingest(OpencodeBackend(), up_to_second_step)

    assert acc.saw_final_message is False
    assert OpencodeBackend().classify(acc, 0) == "failed"


def test_opencode_text_alone_does_not_prove_the_run_finished() -> None:
    """A text part can arrive at any step, so it cannot stand in for a terminal signal."""
    acc = ingest(OpencodeBackend(), OPENCODE_EVENTS[:5])

    assert acc.summary is not None
    assert OpencodeBackend().classify(acc, 0) == "failed"


def test_a_top_level_opencode_error_event_is_a_failure() -> None:
    acc = ingest(OpencodeBackend(), [*OPENCODE_EVENTS, OPENCODE_ERROR])

    assert acc.is_error is True
    assert acc.notices and "UnknownError" in acc.notices[0]
    # Even though the run had already reported `reason: stop`.
    assert OpencodeBackend().classify(acc, 0) == "failed"


@pytest.mark.parametrize(
    ("cost", "why"),
    [(True, "bool is an int, so it would bill $1"),
     (float("nan"), "NaN poisons every later sum"),
     (float("inf"), "infinity poisons every later sum"),
     (-5.0, "a negative cost is not something opencode can truthfully report")],
)
def test_an_unusable_cost_is_dropped_rather_than_accumulated(cost: float, why: str) -> None:
    """json.loads accepts NaN and Infinity by default, so all of these are reachable."""
    acc = ingest(
        OpencodeBackend(),
        [{"type": "step_finish", "sessionID": "ses_x", "part": {"reason": "stop", "cost": cost}}],
    )

    assert acc.total_cost_usd is None, why


def test_an_unusable_token_count_does_not_poison_the_usage_total() -> None:
    acc = ingest(
        OpencodeBackend(),
        [{"type": "step_finish", "sessionID": "ses_x",
          "part": {"reason": "tool-calls", "tokens": {"input": 10}}},
         {"type": "step_finish", "sessionID": "ses_x",
          "part": {"reason": "stop", "tokens": {"input": float("inf"), "output": 3}}}],
    )

    assert acc.usage == {"input": 10, "output": 3}


def test_opencode_nonzero_exit_is_a_failure() -> None:
    acc = ingest(OpencodeBackend(), OPENCODE_EVENTS)

    assert OpencodeBackend().classify(acc, 1) == "failed"


# Captured from real `vibe` 2.25.1 runs made during planning (probes under the session scratchpad's
# `vibeprobe/`), trimmed to the fields `ingest` actually reads — sessionId, type, role, turnId,
# source, content. Real ids and text, not invented.

VIBE_SESSION = "36e00c2b-1fdf-feed-8748-b9b529f9ecc8"
VIBE_FRESH_TURN = "fffd0e88-3784-4be0-b21e-0564efe02fd6"

VIBE_FRESH_RUN = [
    {"type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": VIBE_FRESH_TURN,
     "source": "turn_start",
     "content": [{"type": "text", "text": "Reply with exactly the word OK and nothing else."}]},
    {"type": "reasoning", "sessionId": VIBE_SESSION, "turnId": VIBE_FRESH_TURN,
     "text": "The user wants me to reply with exactly the word \"OK\"..."},
    {"type": "message", "role": "assistant", "sessionId": VIBE_SESSION, "turnId": VIBE_FRESH_TURN,
     "source": None, "content": [{"type": "text", "text": "OK"}]},
]

# A `--resume` run: the prior turn replayed (turnId: null, source: "harness"), a resume checkpoint,
# then the live turn (real turnId, source: "turn_start" on its opening user message).
VIBE_LIVE_TURN = "d19bbb84-150d-4782-96d5-5f26079c2549"

VIBE_RESUMED_RUN = [
    {"type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": None,
     "source": "harness",
     "content": [{"type": "text", "text": "Reply with exactly the word OK and nothing else."}]},
    {"type": "reasoning", "sessionId": VIBE_SESSION, "turnId": None,
     "text": "The user wants me to reply with exactly the word \"OK\"..."},
    {"type": "message", "role": "assistant", "sessionId": VIBE_SESSION, "turnId": None,
     "source": "harness", "content": [{"type": "text", "text": "OK"}]},
    {"type": "checkpoint", "sessionId": VIBE_SESSION, "turnId": None, "kind": "resume",
     "message": "Session resumed"},
    {"type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": VIBE_LIVE_TURN,
     "source": "turn_start",
     "content": [{"type": "text", "text": "What word did you just reply? Answer in one word."}]},
    {"type": "reasoning", "sessionId": VIBE_SESSION, "turnId": VIBE_LIVE_TURN,
     "text": "The user is asking me what word I just replied..."},
    {"type": "message", "role": "assistant", "sessionId": VIBE_SESSION, "turnId": VIBE_LIVE_TURN,
     "source": None, "content": [{"type": "text", "text": "OK"}]},
]

# Gate 1: `--max-turns 1` on a prompt needing a tool call. Exit 1, and the live-turn assistant
# message itself carries the <vibe_stop_event> marker `ingest` must not mistake for a real answer.
VIBE_BREACH_SESSION = "ec5af789-b092-e1e4-24c7-239903609c17"
VIBE_BREACH_TURN = "5997a179-5c4b-4747-8293-3435cbac916f"

VIBE_TURN_LIMIT_BREACH = [
    {"type": "message", "role": "user", "sessionId": VIBE_BREACH_SESSION, "turnId": VIBE_BREACH_TURN,
     "source": "turn_start",
     "content": [{"type": "text",
                  "text": "Read the file target.txt in this directory and tell me its first line."}]},
    {"type": "reasoning", "sessionId": VIBE_BREACH_SESSION, "turnId": VIBE_BREACH_TURN,
     "text": "The user is asking me to read a file named target.txt..."},
    {"type": "effect", "sessionId": VIBE_BREACH_SESSION, "turnId": VIBE_BREACH_TURN, "title": "bash",
     "detail": {"toolName": "bash", "input": {"command": "head -1 target.txt"}}},
    {"type": "message", "role": "assistant", "sessionId": VIBE_BREACH_SESSION,
     "turnId": VIBE_BREACH_TURN, "source": None,
     "content": [{"type": "text", "text": "<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>"}]},
]


VIBE_DENIED_SESSION = "0fc3973e-693c-1539-003e-4b8985efe4ce"
VIBE_DENIED_TURN = "05702fbe-0af3-468d-8788-955d625a9eee"

# A real `git commit` run against a repo whose bash allowlist did not cover it: programmatic mode
# auto-denied the approval and the run then ended with no assistant message at all, at exit 0.
VIBE_DENIED_RUN = [
    {"type": "message", "role": "user", "sessionId": VIBE_DENIED_SESSION,
     "turnId": VIBE_DENIED_TURN, "source": "turn_start",
     "content": [{"type": "text", "text": "Run: git commit -am wip"}]},
    {"type": "reasoning", "sessionId": VIBE_DENIED_SESSION, "turnId": VIBE_DENIED_TURN,
     "text": "The user wants me to run `git commit -am wip`..."},
    {"type": "callback", "sessionId": VIBE_DENIED_SESSION, "turnId": VIBE_DENIED_TURN,
     "title": "Allow bash?",
     "detail": {"kind": "approval",
                "effect": {"toolName": "bash",
                           "input": {"command": "git commit -am wip"}}}},
    {"type": "effect", "sessionId": VIBE_DENIED_SESSION, "turnId": VIBE_DENIED_TURN,
     "title": "bash", "detail": {"toolName": "bash", "input": {"command": "git commit -am wip"}},
     "state": {"status": "cancelled", "reason": "Cancelled"}},
]


def test_vibe_an_auto_denied_approval_is_reported_not_silently_lost() -> None:
    """The run fails with no output at all, so the denial is the only thing that explains it."""
    acc = ingest(VibeBackend(), VIBE_DENIED_RUN)

    assert acc.saw_final_message is False
    assert acc.summary is None
    assert VibeBackend().classify(acc, 0) == "failed"
    assert acc.denials == [
        {"tool": "bash", "command": "git commit -am wip", "title": "Allow bash?"}
    ]


def test_vibe_a_non_approval_callback_is_not_reported_as_a_denial() -> None:
    acc = ingest(
        VibeBackend(),
        [
            VIBE_DENIED_RUN[0],
            {"type": "callback", "sessionId": VIBE_DENIED_SESSION, "turnId": VIBE_DENIED_TURN,
             "title": "Pick one", "detail": {"kind": "selection"}},
            {"type": "callback", "sessionId": VIBE_DENIED_SESSION, "turnId": VIBE_DENIED_TURN,
             "title": "malformed", "detail": "not a dict"},
        ],
    )

    assert acc.denials == []


def test_vibe_a_replayed_callback_before_any_turn_start_is_not_reported_as_a_denial() -> None:
    """A --resume run replays prior history, including callbacks, before the live turn's marker
    arrives. Without a live turn established, a replayed denial must never surface as one."""
    acc = ingest(
        VibeBackend(),
        [
            {"type": "callback", "sessionId": VIBE_DENIED_SESSION, "turnId": None,
             "title": "Allow bash?",
             "detail": {"kind": "approval",
                        "effect": {"toolName": "bash", "input": {"command": "git commit -am wip"}}}},
        ],
    )

    assert acc.denials == []


def test_vibe_a_callback_with_a_non_matching_turn_id_is_not_reported_as_a_denial() -> None:
    acc = ingest(
        VibeBackend(),
        [
            VIBE_DENIED_RUN[0],  # establishes VIBE_DENIED_TURN as the current, live turn
            {"type": "callback", "sessionId": VIBE_DENIED_SESSION, "turnId": "some-other-turn",
             "title": "Allow bash?",
             "detail": {"kind": "approval",
                        "effect": {"toolName": "bash", "input": {"command": "git commit -am wip"}}}},
        ],
    )

    assert acc.denials == []


def test_vibe_normalisation() -> None:
    acc = ingest(VibeBackend(), VIBE_FRESH_RUN)

    assert acc.session_id == VIBE_SESSION
    assert acc.summary == "OK"
    assert acc.saw_final_message is True
    # Nothing here can be truthfully reported: no terminal event, no cost, no token counts, and
    # counting turn_start records would not correspond to what --max-turns actually caps.
    assert acc.total_cost_usd is None
    assert acc.usage is None
    assert acc.num_turns is None
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_nonzero_exit_is_a_failure_even_with_a_closing_message() -> None:
    acc = ingest(VibeBackend(), VIBE_FRESH_RUN)

    assert VibeBackend().classify(acc, 1) == "failed"


def test_vibe_resumed_run_answers_from_the_live_turn() -> None:
    acc = ingest(VibeBackend(), VIBE_RESUMED_RUN)

    assert acc.session_id == VIBE_SESSION
    assert acc.summary == "OK"
    assert acc.saw_final_message is True
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_replay_trap_truncated_before_the_live_answer_reads_as_failed() -> None:
    """The reason this backend needs care: ingesting the resumed stream truncated before the live
    turn's assistant message must not fall back to the replayed prefix's answer."""
    acc = ingest(VibeBackend(), VIBE_RESUMED_RUN[:-1])

    assert acc.saw_final_message is False
    assert acc.summary is None, "the replayed prefix's assistant message must never leak into it"
    assert VibeBackend().classify(acc, 0) == "failed"


def test_vibe_turn_limit_breach_is_a_notice_not_a_closing_message() -> None:
    acc = ingest(VibeBackend(), VIBE_TURN_LIMIT_BREACH)

    # is_error is deliberately NOT set from this: a real breach already exits 1, and classify() is
    # exit-code-authoritative, so the flag would only ever be redundant or, on a false-positive
    # substring match, dangerously wrong.
    assert acc.is_error is None
    assert acc.notices and "<vibe_stop_event>" in acc.notices[0]
    assert acc.saw_final_message is False
    assert VibeBackend().classify(acc, 1) == "failed"


def test_vibe_text_that_merely_quotes_the_stop_event_token_is_preserved_as_the_answer() -> None:
    """A legitimate answer that quotes or explains the token — entirely possible, this repo's own
    docs do it — must not be discarded as though it were a genuine --max-turns breach."""
    quoting_text = "The <vibe_stop_event> marker appears when a turn cap is hit."
    events = [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "t1",
         "source": "turn_start", "content": [{"type": "text", "text": "explain the marker"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "t1", "source": None,
         "content": [{"type": "text", "text": quoting_text}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary == quoting_text
    assert acc.saw_final_message is True
    assert acc.notices == []
    assert VibeBackend().classify(acc, 0) == "completed"


def _vibe_answer(text: str, *, turn: str = "t1") -> list[dict[str, Any]]:
    return [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": turn,
         "source": "turn_start", "content": [{"type": "text", "text": "q"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": turn, "source": None,
         "content": [{"type": "text", "text": text}]},
    ]


def test_vibe_stop_event_match_tolerates_surrounding_whitespace() -> None:
    acc = ingest(VibeBackend(), _vibe_answer("  <vibe_stop_event>limit hit</vibe_stop_event>\n"))

    assert acc.saw_final_message is False
    assert acc.summary is None
    assert acc.notices


@pytest.mark.parametrize(
    "text",
    [
        "Here is what happened: <vibe_stop_event>limit hit</vibe_stop_event>",
        "<vibe_stop_event>limit hit</vibe_stop_event> — so I stopped early.",
        "a <vibe_stop_event>limit hit</vibe_stop_event> b",
    ],
    ids=["leading-prose", "trailing-prose", "both"],
)
def test_vibe_a_stop_event_envelope_with_prose_around_it_is_a_real_answer(text: str) -> None:
    """The anchoring is what separates a genuine breach from an answer that merely contains one."""
    acc = ingest(VibeBackend(), _vibe_answer(text))

    assert acc.summary == text
    assert acc.saw_final_message is True
    assert acc.notices == []
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_a_breach_after_a_normal_answer_withdraws_the_earlier_answer() -> None:
    """Why `ingest` withdraws the evidence rather than merely ignoring the marker.

    An earlier step answers normally, then the turn cap is hit. Merely ignoring the marker would
    leave that earlier answer standing as the run's summary with `saw_final_message` still true — so
    at any zero exit the breach reads as a clean completion. The assertion below uses 0 directly:
    the withdrawal has to hold on its own, independently of what any caller passes.
    """
    events = [
        *_vibe_answer("Partway through, here is what I found."),
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "t1", "source": None,
         "content": [{"type": "text",
                      "text": "<vibe_stop_event>Turn limit of 2 reached</vibe_stop_event>"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary is None
    assert acc.saw_final_message is False
    assert VibeBackend().classify(acc, 1) == "failed"
    # The one that actually matters: a zero exit must not turn the breach into a completion.
    assert VibeBackend().classify(acc, 0) == "failed"


def test_vibe_an_answer_after_a_breach_cannot_re_establish_a_clean_close() -> None:
    events = [
        *_vibe_answer("<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>"),
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "t1", "source": None,
         "content": [{"type": "text", "text": "actually here is an answer"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary is None
    assert acc.saw_final_message is False
    assert VibeBackend().classify(acc, 0) == "failed"


def test_vibe_replayed_entries_with_a_stamped_turn_id_are_still_ignored() -> None:
    """Marker-first, not null-based: a replayed entry with a *non-null* turnId must still be
    ignored, because no turn_start has opened it as the current turn. Stronger than treating
    `turnId is None` as the discriminator, which is what real replayed entries happen to carry."""
    acc = ingest(
        VibeBackend(),
        [{"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "stale-turn",
          "source": "harness", "content": [{"type": "text", "text": "stale answer"}]}],
    )

    assert acc.summary is None
    assert acc.saw_final_message is False


def test_vibe_an_assistant_entry_before_any_marker_is_ignored() -> None:
    acc = ingest(
        VibeBackend(),
        [{"type": "message", "role": "assistant", "sessionId": "s1", "turnId": None,
          "source": None, "content": [{"type": "text", "text": "premature"}]}],
    )

    assert acc.summary is None
    assert acc.saw_final_message is False


@pytest.mark.parametrize("bad_turn_id", [{"weird": "dict"}, ["a", "list"]])
def test_vibe_a_non_string_turn_id_is_never_accepted_as_the_current_turn(bad_turn_id) -> None:
    """A dict or list must not pass the marker's truthiness check and become — or match as — the
    current live turn, even though it would satisfy a bare `if turn_id:` guard."""
    events = [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": bad_turn_id,
         "source": "turn_start", "content": [{"type": "text", "text": "q"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": bad_turn_id,
         "source": None, "content": [{"type": "text", "text": "a"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary is None
    assert acc.saw_final_message is False


def test_vibe_the_same_turn_start_marker_twice_does_not_wipe_a_valid_answer() -> None:
    events = [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "t1",
         "source": "turn_start", "content": [{"type": "text", "text": "q"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "t1", "source": None,
         "content": [{"type": "text", "text": "answer"}]},
        # The same marker arriving again — a duplicated event, say — must not reset the turn.
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "t1",
         "source": "turn_start", "content": [{"type": "text", "text": "q"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary == "answer"
    assert acc.saw_final_message is True
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_a_new_turn_start_resets_every_turn_scoped_field() -> None:
    """Each of the five resets must be independently load-bearing.

    Every field is seeded non-default first, and the new marker is the *last* event — so nothing
    downstream can re-establish a value and mask a reset that never happened. Deleting any one of
    the five reset statements must fail this test.
    """
    backend = VibeBackend()
    acc = Accumulator()
    acc.stream_state["current_turn_id"] = "turn-a"
    acc.stream_state["stop_event_seen"] = True
    acc.summary = "stale answer from the previous turn"
    acc.saw_final_message = True
    acc.is_error = True
    acc.notices = ["stale notice"]
    acc.denials = [{"tool": "bash", "command": "rm -rf /"}]

    backend.ingest(
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "turn-b",
         "source": "turn_start", "content": [{"type": "text", "text": "now do something safe"}]},
        acc,
    )

    assert acc.summary is None
    assert acc.saw_final_message is False
    assert acc.is_error is None
    assert acc.notices == []
    assert acc.denials == []
    assert acc.stream_state["current_turn_id"] == "turn-b"
    # The stop-event latch is turn-scoped too, or a breach in turn-a would silently suppress
    # turn-b's answer.
    assert not acc.stream_state.get("stop_event_seen")


def test_vibe_a_breach_in_an_earlier_turn_does_not_suppress_a_later_turns_answer() -> None:
    events = [
        *_vibe_answer("<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>", turn="turn-a"),
        *_vibe_answer("done", turn="turn-b"),
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary == "done"
    assert acc.saw_final_message is True
    assert acc.notices == []
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_a_denied_first_turn_does_not_leak_into_a_later_successful_turn() -> None:
    events = [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "turn-a",
         "source": "turn_start", "content": [{"type": "text", "text": "run something risky"}]},
        {"type": "callback", "sessionId": "s1", "turnId": "turn-a", "title": "Allow bash?",
         "detail": {"kind": "approval",
                    "effect": {"toolName": "bash", "input": {"command": "rm -rf /"}}}},
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "turn-b",
         "source": "turn_start", "content": [{"type": "text", "text": "now do something safe"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "turn-b",
         "source": None, "content": [{"type": "text", "text": "done"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.denials == []
    assert acc.summary == "done"
    assert acc.saw_final_message is True
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_a_later_distinct_turn_start_is_the_one_that_decides_the_outcome() -> None:
    events = [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "turn-a",
         "source": "turn_start", "content": [{"type": "text", "text": "first question"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "turn-a",
         "source": None, "content": [{"type": "text", "text": "answer a"}]},
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "turn-b",
         "source": "turn_start", "content": [{"type": "text", "text": "second question"}]},
        {"type": "message", "role": "assistant", "sessionId": "s1", "turnId": "turn-b",
         "source": None, "content": [{"type": "text", "text": "answer b"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.summary == "answer b"
    assert VibeBackend().classify(acc, 0) == "completed"


def test_vibe_a_live_turn_with_no_assistant_message_is_a_failure() -> None:
    events = [
        {"type": "message", "role": "user", "sessionId": "s1", "turnId": "turn-a",
         "source": "turn_start", "content": [{"type": "text", "text": "do something"}]},
        {"type": "reasoning", "sessionId": "s1", "turnId": "turn-a", "text": "thinking..."},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.saw_final_message is False
    assert VibeBackend().classify(acc, 0) == "failed"


def test_vibe_session_id_is_the_first_one_sighted() -> None:
    events = [
        {"type": "message", "role": "user", "sessionId": "first-seen", "turnId": "t1",
         "source": "turn_start", "content": [{"type": "text", "text": "q"}]},
        {"type": "message", "role": "assistant", "sessionId": "second-seen", "turnId": "t1",
         "source": None, "content": [{"type": "text", "text": "a"}]},
    ]
    acc = ingest(VibeBackend(), events)

    assert acc.session_id == "first-seen"


def test_vibe_concurrent_accumulators_through_one_shared_backend_do_not_contaminate() -> None:
    """BACKENDS holds one VibeBackend instance shared by every task, so live-turn state must live on
    the Accumulator, never on `self` — otherwise two concurrent tasks would corrupt each other."""
    backend = VibeBackend()
    acc_a, acc_b = Accumulator(), Accumulator()

    backend.ingest(
        {"type": "message", "role": "user", "sessionId": "sa", "turnId": "ta",
         "source": "turn_start", "content": [{"type": "text", "text": "task a"}]},
        acc_a,
    )
    backend.ingest(
        {"type": "message", "role": "user", "sessionId": "sb", "turnId": "tb",
         "source": "turn_start", "content": [{"type": "text", "text": "task b"}]},
        acc_b,
    )
    # Interleaved on purpose: b's answer arrives while a's turn is still the "current" one anywhere
    # state might have leaked.
    backend.ingest(
        {"type": "message", "role": "assistant", "sessionId": "sb", "turnId": "tb",
         "source": None, "content": [{"type": "text", "text": "answer b"}]},
        acc_b,
    )
    backend.ingest(
        {"type": "message", "role": "assistant", "sessionId": "sa", "turnId": "ta",
         "source": None, "content": [{"type": "text", "text": "answer a"}]},
        acc_a,
    )

    assert acc_a.summary == "answer a"
    assert acc_b.summary == "answer b"
    assert acc_a.stream_state["current_turn_id"] == "ta"
    assert acc_b.stream_state["current_turn_id"] == "tb"


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_unrecognised_events_are_ignored_not_fatal(backend) -> None:
    acc = Accumulator()
    for event in ({"type": "something.new"}, {}, {"type": "item.completed", "item": "not a dict"}):
        backend.ingest(event, acc)


# --- capability enforcement ----------------------------------------------------------------


def test_turn_cap_on_a_backend_without_one_is_an_error() -> None:
    """Silently dropping it would leave the caller believing a cap applied."""
    with pytest.raises(backends.UnsupportedCapability, match="no turn cap"):
        backends.reject_turn_cap(CodexBackend(), 10)


def test_turn_cap_is_fine_where_supported() -> None:
    backends.reject_turn_cap(ClaudeBackend(), 10)
    backends.reject_turn_cap(CodexBackend(), None)


def test_unknown_freedom_is_rejected() -> None:
    with pytest.raises(backends.UnsupportedCapability, match="unknown freedom"):
        backends.check_freedom("whatever")


# --- an unobserved exit code -----------------------------------------------------------------
# `classify` takes `int | None`; None means nothing observed the process exit, which is what a
# recovered run looks like. store.py used to substitute 0 here, so every backend appeared to have
# exited cleanly and a recovered run with a closing message was published as `completed` on no
# evidence. What None is worth differs per backend, deliberately.


def test_claude_completes_without_an_observed_exit_because_its_result_event_is_terminal() -> None:
    acc = ingest(ClaudeBackend(), CLAUDE_EVENTS)

    assert ClaudeBackend().classify(acc, None) == "completed"
    # An *observed* non-zero exit still overrules the event.
    assert ClaudeBackend().classify(acc, 1) == "failed"


def test_opencode_completes_without_an_observed_exit_because_stop_is_a_real_end_of_run() -> None:
    acc = ingest(OpencodeBackend(), OPENCODE_EVENTS)

    assert acc.saw_final_message is True
    assert OpencodeBackend().classify(acc, None) == "completed"
    assert OpencodeBackend().classify(acc, 1) == "failed"


@pytest.mark.parametrize(
    ("backend", "events"),
    [(CodexBackend(), CODEX_EVENTS), (VibeBackend(), VIBE_FRESH_RUN)],
    ids=["codex", "vibe"],
)
def test_a_backend_with_no_terminal_event_needs_an_observed_zero_exit(backend, events) -> None:
    """The agent having spoken is not proof the run finished, so None cannot mean completed."""
    acc = ingest(backend, events)

    assert acc.saw_final_message is True, "the fixture must carry a closing message"
    assert backend.classify(acc, 0) == "completed"
    assert backend.classify(acc, None) == "failed"


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
def test_every_backend_accepts_an_unobserved_exit_code_without_raising(backend) -> None:
    """The signature is `int | None` for all four, whatever each one concludes from it."""
    assert backend.classify(Accumulator(), None) in {
        "completed", "failed", "timed_out", "cancelled", "running",
    }
