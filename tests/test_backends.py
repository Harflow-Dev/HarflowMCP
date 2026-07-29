"""The backend seam: argv shapes, freedom mapping, honest enforcement, stream normalisation.

Stream fixtures below are events captured from real runs, not invented.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polybridge import backends
from polybridge.backends import Accumulator, FREEDOMS
from polybridge.backends.claude import DISALLOWED_TOOLS, FORBIDDEN_FLAGS, ClaudeBackend
from polybridge.backends.claude import UnsafeInvocationError as ClaudeUnsafe
from polybridge.backends.codex import NEVER_ASK, CodexBackend
from polybridge.backends.codex import UnsafeInvocationError as CodexUnsafe

REPO = Path("/tmp/repo")
SESSION = "11111111-1111-1111-1111-111111111111"

ALL = [ClaudeBackend(), CodexBackend()]


def start(backend, **kwargs):
    session_id = SESSION if backend.capabilities.chooses_session_id else None
    args = {
        "repo": REPO,
        "freedom": "write_in_repo",
        "session_id": session_id,
        "model": None,
        "max_turns": None,
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
    } | kwargs
    return backend.build_resume_argv("more", **args)


# --- registry ------------------------------------------------------------------------------


def test_both_backends_are_registered() -> None:
    assert sorted(backends.BACKENDS) == ["claude", "codex"]


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
    backend.assert_safe(start(backend, freedom=freedom))
    backend.assert_safe(resume(backend, freedom=freedom))


@pytest.mark.parametrize("backend", ALL, ids=lambda b: b.name)
@pytest.mark.parametrize("prompt", ["", "   "])
def test_empty_prompts_are_rejected(backend, prompt: str) -> None:
    session_id = SESSION if backend.capabilities.chooses_session_id else None
    with pytest.raises(ValueError):
        backend.build_start_argv(
            prompt, repo=REPO, freedom="write_in_repo", session_id=session_id, model=None,
            max_turns=None,
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
    assert "sonnet" not in " ".join(start(backend))
    assert "sonnet" in " ".join(start(backend, model="sonnet"))


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
        ClaudeBackend().assert_safe(argv + [flag])


def test_claude_rejects_a_weakened_deny_list() -> None:
    argv = start(ClaudeBackend())
    argv[argv.index("--disallowedTools") + 1] = "Bash(git push:*)"
    with pytest.raises(ClaudeUnsafe):
        ClaudeBackend().assert_safe(argv)


def test_a_claude_prompt_that_looks_like_a_flag_is_not_mistaken_for_one() -> None:
    argv = ClaudeBackend().build_start_argv(
        "--disallowedTools", repo=REPO, freedom="write_in_repo", session_id=SESSION, model=None,
        max_turns=None,
    )
    ClaudeBackend().assert_safe(argv)


def test_claude_turn_cap_is_emitted() -> None:
    argv = start(ClaudeBackend(), max_turns=7)
    assert argv[argv.index("--max-turns") + 1] == "7"


# --- codex specifics -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("freedom", "mode"),
    [("read_only", "read-only"), ("write_in_repo", "workspace-write"),
     ("unrestricted", "danger-full-access")],
)
def test_codex_freedom_maps_to_a_sandbox_mode(freedom: str, mode: str) -> None:
    argv = start(CodexBackend(), freedom=freedom)
    assert argv[argv.index("-s") + 1] == mode


def test_codex_pins_never_ask_or_it_could_hang() -> None:
    """A headless run that stops for approval waits forever, so this is not optional."""
    argv = start(CodexBackend())
    assert NEVER_ASK[1] in argv
    with pytest.raises(CodexUnsafe, match="approval"):
        CodexBackend().assert_safe([a for a in argv if a != NEVER_ASK[1]])


def test_codex_rejects_a_later_approval_override_that_would_win() -> None:
    argv = with_extra_options(start(CodexBackend()), "-c", 'approval_policy="on-request"')
    with pytest.raises(CodexUnsafe, match="exactly one"):
        CodexBackend().assert_safe(argv)


def test_codex_rejects_a_duplicate_sandbox_under_its_long_alias() -> None:
    """`-s` and `--sandbox` are the same option, so a second one would silently win."""
    argv = with_extra_options(start(CodexBackend()), "--sandbox", "danger-full-access")
    with pytest.raises(CodexUnsafe, match="exactly one sandbox"):
        CodexBackend().assert_safe(argv)


def test_codex_separates_the_prompt_so_it_cannot_be_parsed_as_options() -> None:
    argv = CodexBackend().build_start_argv(
        "--sandbox", repo=REPO, freedom="read_only", session_id=None, model=None, max_turns=None
    )
    assert argv[-2:] == ["--", "--sandbox"]
    # And prompt text must not be able to satisfy a safety check.
    with pytest.raises(CodexUnsafe, match="exactly one sandbox"):
        CodexBackend().assert_safe([a for a in argv if a not in ("-s", "read-only")])


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
    with pytest.raises(CodexUnsafe, match="discards the sandbox"):
        CodexBackend().assert_safe(
            start(CodexBackend()) + ["--dangerously-bypass-approvals-and-sandbox"]
        )


def test_codex_prompt_is_last_so_no_option_swallows_it() -> None:
    assert start(CodexBackend())[-1] == "do a thing"
    # `codex exec resume [SESSION_ID] [PROMPT]` — both positional, in that order.
    assert resume(CodexBackend())[-3:] == ["--", "abc-123", "more"]


def test_codex_working_directory_is_explicit() -> None:
    """Codex takes its root as a flag rather than inheriting the spawn cwd."""
    argv = start(CodexBackend())
    assert argv[argv.index("-C") + 1] == str(REPO)


def test_codex_resume_needs_a_session_id() -> None:
    with pytest.raises(ValueError, match="thread id"):
        resume(CodexBackend(), session_id="")


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
