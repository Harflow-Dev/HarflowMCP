import pytest

from harflow_mcp import background_idle
from harflow_mcp.backends import Accumulator
from harflow_mcp.backends.base import Invocation
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.tasks import background_idle_bound_reached


@pytest.mark.parametrize("value", [True, False, 0, -1, 21601, float("inf"), "5"])
def test_idle_bound_rejects_non_positive_bool_non_numeric_and_over_cap(value):
    with pytest.raises(ValueError):
        background_idle.validate(value)


def test_idle_bound_resolves_environment_once_and_inheritance_wins(monkeypatch):
    monkeypatch.setenv("HARFLOW_LIVE_IDLE_SECONDS", "17")
    assert background_idle.resolve() == 17.0
    assert background_idle.resolve(inherited=23) == 23.0
    assert background_idle.resolve(31, inherited=23) == 31.0


def test_idle_bound_falls_back_for_invalid_environment(monkeypatch):
    monkeypatch.setenv("HARFLOW_LIVE_IDLE_SECONDS", "not-a-number")
    assert background_idle.resolve() == background_idle.DEFAULT_SECONDS


def test_idle_bound_requires_completed_result_and_only_background_work():
    acc = Accumulator()
    acc.result_count = 1
    acc.background_open = {"job"}
    acc.turn_open = False
    acc.awaiting_input = False
    assert background_idle_bound_reached(acc, 10, 10)
    acc.turn_open = True
    assert not background_idle_bound_reached(acc, 10, 10)
    acc.turn_open = False
    acc.result_count = 0
    assert not background_idle_bound_reached(acc, 10, 10)


def test_claude_live_dispatch_context_discloses_effective_bound_without_replacing_prompt():
    original = b'{"message":{"content":[{"type":"text","text":"caller prose"}]}}\n'
    invocation = Invocation(["claude"], stdin_mode="pipe", initial_input=original)
    configured = ClaudeBackend().configure_dispatch_context(invocation, 42)
    assert "caller prose" in configured.initial_input.decode()
    assert "42 seconds" in configured.initial_input.decode()
