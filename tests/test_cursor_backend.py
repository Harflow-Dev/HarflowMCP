"""Parser tests for Cursor stream-json events.

The success object below is the shape the 2026.10.22-27dcf5f writer emits, reconstructed
from that CLI's print function because a captured transcript is not required to classify
a run. A live print-mode run on 2026-10-28 confirmed the process can exit 0; classification
still requires both that exit and a success result. An unobserved exit stays failed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from harflow_mcp.backends import Accumulator, UnsupportedCapability
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
