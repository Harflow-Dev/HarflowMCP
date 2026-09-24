"""Live input (monitor stage A3): a claude run whose stdin stays open for `send_message`.

Unit level — no real agent CLI. Where a real process is needed, a double spawns `/bin/sh` or the
scripted `tests/fake_claude_agent.py`, which speaks claude's stream-json on both pipes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from polybridge import backends, store
from polybridge.backends import STDIN_PIPE, Invocation
from polybridge.backends.base import Enforcement
from polybridge.events import events_path
from polybridge.tasks import TaskRegistry

OWNER = {"pid": 1, "start_time": "t", "markers": []}


class _CatDouble:
    """Spawns `cat > <file>` on a stdin pipe, so what reaches the run's stdin can be read back."""

    name = "live-cat-double"
    binary = "/bin/sh"
    capabilities = backends.ClaudeBackend.capabilities

    def __init__(self, sink: Path) -> None:
        self.sink = sink

    def build_start_argv(self, prompt, **kwargs):
        return Invocation(
            [self.binary, "-c", f"cat > {self.sink}"],
            stdin_mode=STDIN_PIPE,
            initial_input=self.encode_live_message(prompt),
        )

    build_resume_argv = build_start_argv

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

    def ingest(self, event, acc):
        return None

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return "completed" if exit_code == 0 else "failed"

    def encode_live_message(self, text):
        return backends.ClaudeBackend().encode_live_message(text)


def _events(log_dir: Path, task_id: str) -> list[dict]:
    return [json.loads(line) for line in events_path(log_dir, task_id).read_text().splitlines()]


async def test_a_live_invocation_is_spawned_on_a_pipe_carrying_its_prompt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = tmp_path / "stdin.txt"
    backend = _CatDouble(sink)
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner=OWNER)

    task = await registry.start("hello there", tmp_path, backend=backend)
    await task.done.wait()

    assert task.live_input is True
    assert task.status == "completed"
    line = json.loads(sink.read_text())
    assert line["message"]["content"][0]["text"] == "hello there"

    record = store.read(registry.log_dir, task.task_id)
    assert record is not None and record.live_input is True
    assert task.brief()["live_input"] is True
    assert store.brief(registry.log_dir, record)["live_input"] is True
    assert store.snapshot(registry.log_dir, record)["live_input"] is True

    events = _events(registry.log_dir, task.task_id)
    assert events[0]["kind"] == "task_started" and events[0]["live_input"] is True
    assert events[1]["kind"] == "user_message"
    assert events[1]["source"] == "initial"
    assert events[1]["text"] == "hello there"


async def test_a_devnull_invocation_never_gets_a_pipe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`cat` on DEVNULL reads EOF at once: nothing is written and the task is not live."""
    sink = tmp_path / "stdin.txt"

    class _Devnull(_CatDouble):
        name = "devnull-cat-double"

        def build_start_argv(self, prompt, **kwargs):
            return Invocation([self.binary, "-c", f"cat > {self.sink}"])

    backend = _Devnull(sink)
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner=OWNER)

    task = await registry.start("hello", tmp_path, backend=backend)
    await task.done.wait()

    assert task.live_input is False
    assert task.proc.stdin is None
    assert sink.read_text() == ""
    record = store.read(registry.log_dir, task.task_id)
    assert record is not None and record.live_input is False
    kinds = [event["kind"] for event in _events(registry.log_dir, task.task_id)]
    assert "user_message" not in kinds


def test_a_record_written_before_live_input_existed_loads_as_not_live(tmp_path: Path) -> None:
    path = store.record_path(tmp_path, "old")
    path.write_text(
        json.dumps(
            {
                "task_id": "old",
                "backend": "claude",
                "session_id": "s",
                "repo_path": str(tmp_path),
                "started_at": "2026-01-01T00:00:00+00:00",
                "status": "completed",
                "exit_code": 0,
            }
        )
    )
    record = store.read(tmp_path, "old")
    assert record is not None and record.live_input is False


# --- A3.2: the turn and background seam (claude ingest) ----------------------------------------
#
# Minimal copies of events measured from claude 2.1.281 (see tests/test_live_input_real.py), keeping
# only the fields the seam reads.


def _started(task_id: str, backgrounded: bool) -> dict:
    return {
        "type": "system",
        "subtype": "task_started",
        "task_id": task_id,
        "tool_use_id": "toolu_x",
        "is_backgrounded": backgrounded,
        "task_type": "local_bash",
    }


def _updated(task_id: str, status: str | None = None, **patch) -> dict:
    if status is not None:
        patch["status"] = status
    return {"type": "system", "subtype": "task_updated", "task_id": task_id, "patch": patch}


def _notification(task_id: str, status: str) -> dict:
    return {"type": "system", "subtype": "task_notification", "task_id": task_id, "status": status}


def _result(text: str = "ok", *, subtype: str = "success", is_error: bool = False, **extra) -> dict:
    return {"type": "result", "subtype": subtype, "is_error": is_error, "result": text, **extra}


ASSISTANT = {"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}}
TOOL_RESULT = {"type": "user", "message": {"content": [{"type": "tool_result", "content": "x"}]}}


def _feed(*events: dict):
    from polybridge.backends import Accumulator

    acc = Accumulator()
    claude = backends.ClaudeBackend()
    for event in events:
        claude.ingest(event, acc)
    return acc


def test_a_result_with_nothing_in_the_background_awaits_input() -> None:
    acc = _feed(ASSISTANT, _result())
    assert acc.awaiting_input and not acc.turn_open and acc.result_count == 1


def test_a_foreground_task_never_opens_a_background_entry() -> None:
    """Measured: a foreground Bash emits task_started (is_backgrounded false) and its own
    task_notification, so neither may keep the run from going idle."""
    acc = _feed(ASSISTANT, _started("fg", False), TOOL_RESULT, _notification("fg", "completed"))
    assert acc.background_open == set()
    acc = _feed(ASSISTANT, _started("fg", False), TOOL_RESULT, _result())
    assert acc.background_open == set() and acc.awaiting_input


def test_an_open_background_task_keeps_the_run_from_idling_until_it_finishes() -> None:
    acc = _feed(ASSISTANT, _started("bg", True), TOOL_RESULT, ASSISTANT, _result("STARTED"))
    assert acc.background_open == {"bg"}
    assert not acc.awaiting_input

    claude = backends.ClaudeBackend()
    claude.ingest(_updated("bg", "completed", end_time=1), acc)
    assert acc.background_open == set()
    assert acc.awaiting_input
    # The follow-up turn claude starts by itself is turn activity, then its own result.
    claude.ingest(_notification("bg", "completed"), acc)
    claude.ingest(ASSISTANT, acc)
    assert not acc.awaiting_input and acc.turn_open
    claude.ingest(_result("done"), acc)
    assert acc.awaiting_input and acc.result_count == 2


@pytest.mark.parametrize(
    "closing",
    [
        _updated("bg", "completed"),
        _updated("bg", "killed"),
        _updated("bg", "failed"),
        _notification("bg", "completed"),
        _notification("bg", "stopped"),
        _notification("bg", "error"),
        _notification("bg", "cancelled"),
    ],
    ids=["updated-completed", "updated-killed", "updated-failed", "note-completed", "note-stopped",
         "note-error", "note-cancelled"],
)
def test_a_terminal_status_on_either_event_closes_the_entry(closing: dict) -> None:
    acc = _feed(_started("bg", True), _result(), closing)
    assert acc.background_open == set() and acc.awaiting_input


@pytest.mark.parametrize(
    "event",
    [
        _updated("bg", "running"),
        _updated("bg", None, end_time=5),
        _notification("bg", "weird"),
        _updated("other", "completed"),
        {"type": "system", "subtype": "task_updated", "task_id": "bg", "patch": "nope"},
        {"type": "system", "subtype": "task_notification", "status": "completed"},
    ],
    ids=["running", "no-status", "unknown-note", "other-id", "bad-patch", "no-id"],
)
def test_anything_else_keeps_a_background_entry_open(event: dict) -> None:
    acc = _feed(_started("bg", True), _result(), event)
    assert acc.background_open == {"bg"} and not acc.awaiting_input


def test_a_task_moved_to_the_background_later_counts_as_open() -> None:
    acc = _feed(_started("t", False), _updated("t", None, is_backgrounded=True), _result())
    assert acc.background_open == {"t"} and not acc.awaiting_input


def test_a_background_task_finishing_mid_turn_does_not_idle_the_run() -> None:
    acc = _feed(_started("bg", True), _result("first"), ASSISTANT, _updated("bg", "completed"))
    assert acc.background_open == set()
    assert not acc.awaiting_input  # a turn is running
    backends.ClaudeBackend().ingest(_result("second"), acc)
    assert acc.awaiting_input


def test_a_background_task_finishing_before_any_result_does_not_idle_the_run() -> None:
    acc = _feed(ASSISTANT, _started("bg", True), _updated("bg", "completed"))
    assert acc.background_open == set() and not acc.awaiting_input


@pytest.mark.parametrize("event", [ASSISTANT, TOOL_RESULT])
def test_turn_activity_clears_awaiting_input(event: dict) -> None:
    acc = _feed(_result(), event)
    assert not acc.awaiting_input and acc.turn_open


def test_other_system_events_leave_awaiting_input_alone() -> None:
    acc = _feed(_result(), {"type": "system", "subtype": "background_tasks_changed", "tasks": []})
    assert acc.awaiting_input
    acc = _feed(_result(), {"type": "rate_limit_event"})
    assert acc.awaiting_input


def test_classify_reports_an_abandoned_background_wait_as_failed() -> None:
    claude = backends.ClaudeBackend()
    acc = _feed(_started("bg", True), _result("STARTED"))
    assert claude.classify(acc, 0) == "completed"
    acc.background_abandoned = True
    assert claude.classify(acc, 0) == "failed"
    assert claude.classify(acc, None) == "failed"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, 600.0),
        ("30", 30.0),
        ("0.5", 0.5),
        ("0", 600.0),
        ("-5", 600.0),
        ("nan", 600.0),
        ("inf", 600.0),
        ("soon", 600.0),
        ("", 600.0),
    ],
)
def test_live_idle_seconds_parsing(raw, expected: float) -> None:
    from polybridge.tasks import live_idle_seconds

    environ = {} if raw is None else {"PB_LIVE_IDLE_SECONDS": raw}
    assert live_idle_seconds(environ) == expected


def test_the_idle_bound_applies_only_to_a_wait_on_background_tasks_alone() -> None:
    from polybridge.tasks import background_idle_bound_reached as reached

    waiting = _feed(_started("bg", True), _result())
    assert reached(waiting, silent_for=600, bound=600)
    assert not reached(waiting, silent_for=599.9, bound=600)

    assert not reached(_feed(_result()), silent_for=10_000, bound=600)  # idle, nothing open
    assert not reached(_feed(_started("bg", True)), silent_for=10_000, bound=600)  # no result yet
    working = _feed(_started("bg", True), _result(), ASSISTANT)
    assert not reached(working, silent_for=10_000, bound=600)  # a turn is running
