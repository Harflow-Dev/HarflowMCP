"""Parser tests for Cursor stream-json events.

The success object below is the shape the 2026.10.22-27dcf5f writer emits, reconstructed
from that CLI's print function because a captured transcript is not required to classify
a run. A live print-mode run on 2026-10-28 confirmed the process can exit 0; classification
still requires both that exit and a success result. An unobserved exit stays failed.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from harflow_mcp.backends import Accumulator, Backend, UnsupportedCapability
from harflow_mcp.backends.cursor import CursorBackend

FIXTURE = Path(__file__).parent / "fixtures" / "cursor" / "success-result.jsonl"
BACKEND = CursorBackend()


def test_success_result_and_a_zero_exit_completes() -> None:
    acc = Accumulator()
    for line in FIXTURE.read_text().splitlines():
        BACKEND.ingest(json.loads(line), acc)
    assert acc.session_id == "11111111-1111-4111-8111-111111111111"
    assert acc.summary == "pong"
    assert BACKEND.classify(acc, 0) == "completed"


def test_an_unobserved_exit_is_not_invented_as_success() -> None:
    acc = Accumulator()
    for line in FIXTURE.read_text().splitlines():
        BACKEND.ingest(json.loads(line), acc)
    assert BACKEND.classify(acc, None) == "failed"


def test_a_nonzero_exit_fails_even_with_a_success_result() -> None:
    acc = Accumulator()
    BACKEND.ingest(json.loads(FIXTURE.read_text().splitlines()[-1]), acc)
    assert BACKEND.classify(acc, 1) == "failed"


def test_stderr_only_failure_has_no_result_and_fails() -> None:
    assert BACKEND.classify(Accumulator(), 1) == "failed"


@pytest.mark.parametrize("flag", ["--force", "--sandbox", "--effort", "--system-prompt"])
def test_assert_safe_refuses_unmeasured_flags(flag: str) -> None:
    argv = BACKEND.build_start_argv(
        "hello",
        repo=Path("/tmp/repo"),
        freedom="write_in_repo",
        session_id=None,
        model=None,
        max_turns=None,
        reasoning_effort=None,
    ).argv
    cut = argv.index("--")
    tampered = argv[:cut] + [flag, "value"] + argv[cut:]
    from harflow_mcp.backends.base import Invocation
    from harflow_mcp.backends.cursor import UnsafeInvocationError

    with pytest.raises((UnsafeInvocationError, UnsupportedCapability)):
        BACKEND.assert_safe(Invocation(tampered), "write_in_repo")


def test_a_flag_shaped_prompt_stays_after_the_separator() -> None:
    argv = BACKEND.build_start_argv(
        "--max-turns",
        repo=Path("/tmp/repo"),
        freedom="write_in_repo",
        session_id=None,
        model=None,
        max_turns=None,
        reasoning_effort=None,
    ).argv
    assert argv[-2:] == ["--", "--max-turns"]


def test_live_input_is_refused() -> None:
    with pytest.raises(UnsupportedCapability):
        BACKEND.encode_live_message("hello")


def test_fixture_normalizes_assistant_once_without_mutating_accumulator() -> None:
    acc = Accumulator()
    emitted = []
    for line in FIXTURE.read_text().splitlines():
        event = json.loads(line)
        BACKEND.ingest(event, acc)
        before = copy.deepcopy(acc)
        emitted.extend(BACKEND.normalize(event, acc))
        assert acc == before
    assert emitted == [{"kind": "assistant_text", "text": "pong"}]


def test_normalize_preserves_multiple_text_blocks_and_ignores_malformed_blocks() -> None:
    event = {"type": "assistant", "message": {"content": [
        None, "bad", {}, {"type": "text", "text": 7},
        {"type": "tool_use", "text": "ignore"},
        {"type": "text", "text": "first"},
        {"type": "text", "text": "second"},
    ]}}
    acc = Accumulator(summary="existing", stream_state={"existing": [1]})
    before = copy.deepcopy(acc)
    assert BACKEND.normalize(event, acc) == [
        {"kind": "assistant_text", "text": "first"},
        {"kind": "assistant_text", "text": "second"},
    ]
    assert acc == before


@pytest.mark.parametrize("event", [
    {}, {"type": "unknown", "message": {"content": [{"type": "text", "text": "ignore"}]}},
    {"type": "assistant"}, {"type": "assistant", "message": None},
    {"type": "assistant", "message": []},
    {"type": "assistant", "message": {"content": "bad"}},
    {"type": "assistant", "message": {"content": {"type": "text", "text": "bad"}}},
    {"type": "result", "result": "ignore"},
])
def test_normalize_ignores_unknown_or_malformed_events(event: dict) -> None:
    acc = Accumulator()
    before = copy.deepcopy(acc)
    assert BACKEND.normalize(event, acc) == []
    assert acc == before


def test_backend_protocol_and_unqualified_signals_are_conservative() -> None:
    assert isinstance(BACKEND, Backend)
    assert BACKEND.stderr_usage_limit_diagnostic("Usage limit exceeded") is None
    assert BACKEND.workflow_stderr_blocks_availability_failure("Usage limit exceeded") is False
    for exit_code in (None, 0, 1):
        assert BACKEND.continuation_reason(Accumulator(), exit_code) is None
