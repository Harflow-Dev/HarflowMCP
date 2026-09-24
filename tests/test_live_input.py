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


RESULT_LINE = '{"type":"result","subtype":"success","is_error":false,"result":"ok"}'


class _CatDouble:
    """Spawns a shell on a stdin pipe that copies the first line it reads to a file, answers with a
    claude `result`, then waits for EOF — the smallest process that behaves like a live agent.
    Ingest and classify are claude's own, so the input pump sees the result and closes stdin."""

    name = "live-cat-double"
    binary = "/bin/sh"
    capabilities = backends.ClaudeBackend.capabilities

    def __init__(self, sink: Path) -> None:
        self.sink = sink

    def build_start_argv(self, prompt, **kwargs):
        return Invocation(
            [self.binary, "-c", f"head -n 1 > {self.sink}; echo '{RESULT_LINE}'; cat > /dev/null"],
            stdin_mode=STDIN_PIPE,
            initial_input=self.encode_live_message(prompt),
        )

    build_resume_argv = build_start_argv

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

    def ingest(self, event, acc):
        backends.ClaudeBackend().ingest(event, acc)

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return backends.ClaudeBackend().classify(acc, exit_code)

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
            return Invocation([self.binary, "-c", f"cat > {self.sink}; echo '{RESULT_LINE}'"])

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


# --- A3.3: accounting across results -------------------------------------------------------------


def test_per_result_counters_accumulate_and_cost_takes_the_latest() -> None:
    acc = _feed(
        _result(
            "first",
            num_turns=2,
            total_cost_usd=0.0074,
            usage={
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_creation_input_tokens": 100,
                "cache_read_input_tokens": 1000,
                "service_tier": "standard",
                "server_tool_use": {"web_search_requests": 1},
            },
            permission_denials=[{"tool_name": "Bash", "tool_input": {"command": "git push"}}],
        ),
        ASSISTANT,
        _result(
            "second",
            num_turns=3,
            total_cost_usd=0.0299,
            usage={
                "input_tokens": 1,
                "output_tokens": 2,
                "cache_creation_input_tokens": 3,
                "cache_read_input_tokens": 4,
                "service_tier": "priority",
                "server_tool_use": {"web_search_requests": 0},
            },
            permission_denials=[
                {"tool_input": {"command": "git push"}, "tool_name": "Bash"},
                {"tool_name": "Write", "tool_input": {"file_path": "/etc/x"}},
            ],
        ),
    )
    assert acc.result_count == 2
    assert acc.num_turns == 5
    assert acc.total_cost_usd == pytest.approx(0.0299)  # cumulative per process: the latest
    assert acc.usage == {
        "input_tokens": 11,
        "output_tokens": 7,
        "cache_creation_input_tokens": 103,
        "cache_read_input_tokens": 1004,
        "service_tier": "priority",
        "server_tool_use": {"web_search_requests": 0},
    }
    # Union, first-seen order, same denial recognised whatever its key order.
    assert acc.denials == [
        {"tool_name": "Bash", "tool_input": {"command": "git push"}},
        {"tool_name": "Write", "tool_input": {"file_path": "/etc/x"}},
    ]
    assert acc.summary == "second"


def test_a_single_result_accounts_exactly_as_before() -> None:
    usage = {"input_tokens": 3, "output_tokens": 4, "service_tier": "standard"}
    acc = _feed(_result("only", num_turns=1, total_cost_usd=0.5, usage=usage, permission_denials=[]))
    assert (acc.num_turns, acc.total_cost_usd, acc.usage, acc.denials) == (1, 0.5, usage, [])
    acc = _feed(_result("only"))
    assert (acc.num_turns, acc.total_cost_usd, acc.usage, acc.denials) == (None, None, None, [])


def test_malformed_counters_never_poison_the_totals() -> None:
    acc = _feed(
        _result(num_turns=2, total_cost_usd=0.1, usage={"input_tokens": 5}),
        _result(num_turns=True, total_cost_usd="lots", usage={"input_tokens": "many"}),
        _result(num_turns="3", usage="nope", permission_denials="none"),
    )
    assert acc.num_turns == 2
    assert acc.total_cost_usd == pytest.approx(0.1)
    assert acc.usage == {"input_tokens": "many"}  # not a count: the latest value, never summed
    assert acc.denials == []


def test_the_worst_outcome_is_sticky() -> None:
    claude = backends.ClaudeBackend()
    acc = _feed(
        _result("broke", subtype="error_during_execution", is_error=True),
        ASSISTANT,
        _result("fine later"),
    )
    assert acc.error_result_seen
    assert acc.terminal["result"] == "broke"
    assert acc.is_error is True
    assert acc.summary == "fine later"
    assert claude.classify(acc, 0) == "failed"

    acc = _feed(_result("fine"), ASSISTANT, _result("capped", subtype="error_max_turns", is_error=True))
    assert acc.error_result_seen and acc.terminal["result"] == "capped"
    assert claude.classify(acc, 0) == "timed_out"


def test_a_non_success_subtype_is_an_error_even_without_is_error() -> None:
    claude = backends.ClaudeBackend()
    acc = _feed(_result("odd", subtype="error_during_execution", is_error=False))
    assert acc.error_result_seen
    assert claude.classify(acc, 0) == "failed"
    acc = _feed(_result("ok"))
    assert not acc.error_result_seen
    assert claude.classify(acc, 0) == "completed"


# --- A3.4: the input pump, the close protocol, and send ------------------------------------------

import asyncio  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402
from collections import deque  # noqa: E402
from datetime import datetime, timezone  # noqa: E402
from types import SimpleNamespace  # noqa: E402

from polybridge import control, identity, inbox  # noqa: E402
from polybridge import tasks as tasks_module  # noqa: E402
from polybridge.tasks import Task  # noqa: E402

FAKE_AGENT = Path(__file__).with_name("fake_claude_agent.py")
SETTLE_SECONDS = 20.0


class _FakeClaude:
    """claude's own ingest/normalize/classify/encoding, driving `tests/fake_claude_agent.py`."""

    name = "fake-claude"
    binary = sys.executable
    capabilities = backends.ClaudeBackend.capabilities

    def __init__(self) -> None:
        self._claude = backends.ClaudeBackend()

    def build_start_argv(self, prompt, **kwargs):
        return Invocation(
            [self.binary, str(FAKE_AGENT), kwargs.get("session_id") or "s"],
            stdin_mode=STDIN_PIPE,
            initial_input=self.encode_live_message(prompt),
        )

    build_resume_argv = build_start_argv

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation) and invocation.live_input

    def enforcement(self, freedom, network=None):
        return self._claude.enforcement(freedom, network)

    def ingest(self, event, acc):
        self._claude.ingest(event, acc)

    def normalize(self, event, acc):
        return self._claude.normalize(event, acc)

    def classify(self, acc, exit_code):
        return self._claude.classify(acc, exit_code)

    def encode_live_message(self, text):
        return self._claude.encode_live_message(text)


@pytest.fixture
def fake(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A registry whose owner is this real process (so a non-owner send finds it alive), with the
    fake agent registered and its stdin log under tmp_path."""
    backend = _FakeClaude()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    stdin_log = tmp_path / "agent-stdin.jsonl"
    monkeypatch.setenv("FAKE_CLAUDE_LOG", str(stdin_log))
    registry = TaskRegistry(log_dir=tmp_path / "streams", owner=identity.own_identity())
    return SimpleNamespace(backend=backend, registry=registry, stdin_log=stdin_log, repo=tmp_path)


async def _settle(task) -> None:
    await asyncio.wait_for(task.done.wait(), SETTLE_SECONDS)


async def _until(predicate, timeout: float = SETTLE_SECONDS) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("condition never held")
        await asyncio.sleep(0.02)


def _received(stdin_log: Path) -> list[str]:
    if not stdin_log.exists():
        return []
    return [
        json.loads(line)["message"]["content"][0]["text"]
        for line in stdin_log.read_text().splitlines()
    ]


def _kinds(registry, task) -> list[str]:
    return [event["kind"] for event in _events(registry.log_dir, task.task_id)]


async def test_a_mid_turn_message_is_folded_into_the_running_turn(fake) -> None:
    task = await fake.registry.start("slow 1.5 first", fake.repo, backend=fake.backend)
    await _until(lambda: task.acc.turn_open)

    queued = await fake.registry.send_message(task, "and this too")
    assert queued["status"] == "queued" and queued["task_id"] == task.task_id

    await _settle(task)
    assert task.status == "completed"
    assert task.acc.result_count == 1
    assert task.acc.summary == "first | folded: and this too"
    assert _received(fake.stdin_log) == ["slow 1.5 first", "and this too"]
    assert task.input_closed and task.inbox_closed
    assert inbox.is_closed(fake.registry.log_dir, task.task_id)

    events = _events(fake.registry.log_dir, task.task_id)
    users = [e for e in events if e["kind"] == "user_message"]
    assert [(e["source"], e["text"]) for e in users] == [
        ("initial", "slow 1.5 first"),
        ("injected", "and this too"),
    ]
    assert users[1]["message_id"] == queued["message_id"]
    assert events[-1]["kind"] == "task_finished"


async def test_a_send_after_the_close_is_refused_with_resume_advice(fake) -> None:
    task = await fake.registry.start("reply hi", fake.repo, backend=fake.backend)
    await _settle(task)
    assert task.status == "completed"

    with pytest.raises(inbox.SendRefused, match="finished; continue with resume_task") as caught:
        await fake.registry.send_message(task, "too late")
    assert caught.value.code == "closed"
    # And from another process, via the on-disk inbox.
    with pytest.raises(inbox.SendRefused, match="finished; continue with resume_task"):
        await fake.registry.send_to_record(task.task_id, "too late")


async def test_a_message_sent_from_another_process_is_delivered(fake) -> None:
    task = await fake.registry.start("slow 2 first", fake.repo, backend=fake.backend)
    await _until(lambda: task.acc.turn_open)

    queued = await asyncio.to_thread(
        inbox.send_to_record, fake.registry.log_dir, task.task_id, "from afar", by=None
    )
    assert queued["status"] == "queued"

    await _settle(task)
    assert task.status == "completed"
    assert "from afar" in task.acc.summary
    user_ids = [
        e.get("message_id")
        for e in _events(fake.registry.log_dir, task.task_id)
        if e["kind"] == "user_message"
    ]
    assert queued["message_id"] in user_ids


async def test_an_open_background_task_holds_input_open_until_it_finishes(fake) -> None:
    task = await fake.registry.start("bg b1 1", fake.repo, backend=fake.backend)
    await _until(lambda: task.acc.result_count == 1)
    assert task.acc.background_open == {"b1"}
    assert not task.input_closed

    # While idle-but-waiting, a message starts a turn of its own.
    await fake.registry.send_message(task, "reply meanwhile")
    await _settle(task)

    assert task.status == "completed"
    assert task.acc.background_open == set()
    assert task.acc.result_count == 3  # STARTED, meanwhile, and the background follow-up turn
    assert task.acc.summary == "bg b1 done"
    assert _received(fake.stdin_log) == ["bg b1 1", "reply meanwhile"]


async def test_the_idle_bound_closes_a_background_wait_and_fails_the_run(
    fake, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PB_LIVE_IDLE_SECONDS", "0.5")
    task = await fake.registry.start("silentbg forever", fake.repo, backend=fake.backend)
    await _settle(task)

    assert task.acc.background_abandoned
    assert task.status == "failed"
    assert task.exit_code == 0
    assert any("abandoned at the idle bound" in notice for notice in task.bridge_notices)
    record = store.read(fake.registry.log_dir, task.task_id)
    assert any("abandoned at the idle bound" in n for n in record.bridge_notices)
    assert "notice" in _kinds(fake.registry, task)


async def test_an_error_result_closes_input_and_the_run_fails(fake) -> None:
    task = await fake.registry.start("error", fake.repo, backend=fake.backend)
    await _settle(task)
    assert task.status == "failed"
    assert task.acc.error_result_seen
    assert task.inbox_closed
    with pytest.raises(inbox.SendRefused, match="finished; continue with resume_task"):
        await fake.registry.send_message(task, "anything")


async def test_a_run_that_exits_without_a_result_fails_and_closes_its_input(fake) -> None:
    task = await fake.registry.start("bg b1 30", fake.repo, backend=fake.backend)
    await _until(lambda: task.acc.result_count == 1)
    await fake.registry.send_message(task, "die 3")
    await _settle(task)

    assert task.exit_code == 3
    assert task.status == "failed"
    assert task.inbox_closed
    with pytest.raises(inbox.SendRefused, match="finished; continue with resume_task"):
        await fake.registry.send_message(task, "after exit")


async def test_messages_queued_when_the_run_exits_are_reported_undelivered(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.proc.returncode = 3
    task.inbox_queue.append(inbox.make_message("never sent", None))

    await asyncio.wait_for(registry._pump(task), 5)

    assert stdin.written == [] and task.inbox_closed
    undelivered = [e for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert [e["text"] for e in undelivered] == ["never sent"]
    assert "exited" in undelivered[0]["reason"]
    assert any("was not delivered" in notice for notice in task.bridge_notices)


async def test_the_monitor_lets_the_pump_report_before_task_finished(fake) -> None:
    """A message accepted while the run is exiting is reported before `task_finished`."""
    task = await fake.registry.start("bg b1 30", fake.repo, backend=fake.backend)
    await _until(lambda: task.acc.result_count == 1)
    original = fake.registry._forward_pending

    async def hold_back(t):  # the pump stops forwarding, so the queued message stays queued
        return False

    fake.registry._forward_pending = hold_back
    await fake.registry.send_message(task, "stuck in the queue")
    os.killpg(task.pgid, 9)
    await _settle(task)
    fake.registry._forward_pending = original

    kinds = _kinds(fake.registry, task)
    assert "undelivered" in kinds
    assert kinds.index("undelivered") < kinds.index("task_finished")


async def test_cancelling_a_live_task_closes_its_input(fake) -> None:
    task = await fake.registry.start("slow 30 x", fake.repo, backend=fake.backend)
    await _until(lambda: task.acc.turn_open)
    await fake.registry.cancel(task)
    await _settle(task)
    assert task.status == "cancelled"
    assert task.inbox_closed
    with pytest.raises(inbox.SendRefused):
        await fake.registry.send_message(task, "after cancel")


async def test_a_classic_task_refuses_a_message(fake, monkeypatch: pytest.MonkeyPatch) -> None:
    class _Classic(_CatDouble):
        name = "classic-double"

        def build_start_argv(self, prompt, **kwargs):
            return Invocation([self.binary, "-c", f"echo '{RESULT_LINE}'"])

    backend = _Classic(fake.repo / "unused")
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    task = await fake.registry.start("x", fake.repo, backend=backend)
    with pytest.raises(inbox.SendRefused, match="not started with live input") as caught:
        await fake.registry.send_message(task, "hi")
    assert caught.value.code == "not_live_input"
    await _settle(task)
    with pytest.raises(inbox.SendRefused) as caught:
        await fake.registry.send_to_record(task.task_id, "hi")
    assert caught.value.code == "not_live_input"


# Unit level: a Task with a stub process, so every branch of the close protocol is deterministic.


class _Stdin:
    def __init__(self, *, fail_write: Exception | None = None, fail_drain: Exception | None = None):
        self.written: list[bytes] = []
        self.closed = False
        self._fail_write = fail_write
        self._fail_drain = fail_drain

    def is_closing(self) -> bool:
        return self.closed

    def write(self, data: bytes) -> None:
        if self._fail_write is not None:
            raise self._fail_write
        self.written.append(data)

    async def drain(self) -> None:
        if self._fail_drain is not None:
            raise self._fail_drain

    def close(self) -> None:
        self.closed = True


def _stub_task(log_dir: Path, stdin: _Stdin, task_id: str = "live1") -> Task:
    from polybridge.events import EventLog

    log_dir.mkdir(parents=True, exist_ok=True)
    task = Task(
        task_id=task_id,
        backend="claude",
        session_id="s",
        repo_path=log_dir,
        prompt="p",
        max_turns=None,
        log_path=log_dir / f"{task_id}.jsonl",
        started_at=datetime.now(timezone.utc),
        live_input=True,
    )
    task.proc = SimpleNamespace(stdin=stdin, returncode=None, pid=None)
    task.events = EventLog(events_path(log_dir, task_id), task_id)
    return task


def _texts(data: list[bytes]) -> list[str]:
    return [json.loads(chunk)["message"]["content"][0]["text"] for chunk in data]


async def test_an_error_result_drops_every_queued_message_as_undelivered(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.inbox_queue.extend([inbox.make_message("one", None), inbox.make_message("two", None)])
    fd = inbox.lock_sync(tmp_path, task.task_id)
    inbox.append_locked(tmp_path, task.task_id, inbox.make_message("three", None))
    inbox.unlock(fd)
    task.acc.error_result_seen = True

    await asyncio.wait_for(registry._pump(task), 5)

    assert stdin.written == [] and stdin.closed
    assert task.inbox_closed and inbox.is_closed(tmp_path, task.task_id)
    undelivered = [e for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert sorted(e["text"] for e in undelivered) == ["one", "three", "two"]
    assert all("reported an error" in e["reason"] for e in undelivered)
    assert sum("was not delivered" in n for n in task.bridge_notices) == 3


async def test_idle_with_nothing_queued_closes_input(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True

    await asyncio.wait_for(registry._pump(task), 5)

    assert stdin.closed and task.inbox_closed and stdin.written == []


async def test_a_send_racing_the_close_is_either_delivered_or_refused(tmp_path: Path) -> None:
    """The close protocol holds the inbox lock from its last read to the marker: a sender that got
    the lock first is forwarded; one that comes after is refused. Simulated by holding the lock (as
    a sender would) while the pump tries to close, appending, then releasing."""
    registry = TaskRegistry(log_dir=tmp_path, owner=identity.own_identity())
    store.write(
        tmp_path,
        store.TaskRecord(
            task_id="live1",
            backend="claude",
            session_id="s",
            repo_path=str(tmp_path),
            started_at=datetime.now(timezone.utc).isoformat(),
            owner=identity.own_identity(),
            live_input=True,
        ),
    )
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True

    fd = inbox.lock_sync(tmp_path, task.task_id)
    pump = asyncio.create_task(registry._pump(task))
    await asyncio.sleep(0.3)
    assert not pump.done() and not stdin.closed  # waiting for the lock, stdin still open
    inbox.append_locked(tmp_path, task.task_id, inbox.make_message("just in time", None))
    inbox.unlock(fd)
    await asyncio.wait_for(pump, 5)

    assert _texts(stdin.written) == ["just in time"]
    assert stdin.closed and inbox.is_closed(tmp_path, task.task_id)
    with pytest.raises(inbox.SendRefused, match="finished; continue with resume_task"):
        inbox.send_to_record(tmp_path, task.task_id, "too late", by=None)
    with pytest.raises(inbox.SendRefused, match="finished; continue with resume_task"):
        await registry.send_message(task, "too late")


async def test_concurrent_senders_never_lose_an_acknowledged_message(tmp_path: Path) -> None:
    """Many senders race a closing pump: every "queued" message ends up written or reported."""
    owner = identity.own_identity()
    registry = TaskRegistry(log_dir=tmp_path, owner=owner)
    store.write(
        tmp_path,
        store.TaskRecord(
            task_id="live1",
            backend="claude",
            session_id="s",
            repo_path=str(tmp_path),
            started_at=datetime.now(timezone.utc).isoformat(),
            owner=owner,
            live_input=True,
        ),
    )
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.owner = owner  # the pump persists the record; it must keep naming its live owner
    task.acc.result_count = 1

    accepted: list[str] = []

    def sender(n: int) -> None:
        for i in range(20):
            try:
                accepted.append(
                    inbox.send_to_record(tmp_path, "live1", f"m{n}-{i}", by=None)["message_id"]
                )
            except inbox.SendRefused as exc:
                assert exc.code == "closed"

    senders = [asyncio.to_thread(sender, n) for n in range(4)]
    pump = asyncio.create_task(registry._pump(task))

    async def close_soon() -> None:
        # Each forward clears awaiting_input (a turn is owed); with no agent to answer, keep
        # re-asserting it the way each turn's result would, until the pump closes.
        await asyncio.sleep(0.05)
        while not pump.done():
            task.acc.awaiting_input = True
            task.pump_wake.set()
            await asyncio.sleep(0.01)

    await asyncio.gather(*senders, close_soon())
    await asyncio.wait_for(pump, 10)

    written = {
        e["message_id"] for e in _events(tmp_path, "live1") if e["kind"] == "user_message"
    }
    reported = {
        e["message_id"] for e in _events(tmp_path, "live1") if e["kind"] == "undelivered"
    }
    assert accepted, "no send was accepted before the close — the race was not exercised"
    assert set(accepted) <= written | reported
    assert len(_texts(stdin.written)) == len(written)


@pytest.mark.parametrize("error", [BrokenPipeError(), ConnectionResetError()])
async def test_a_broken_pipe_on_write_reports_the_rest_undelivered(tmp_path: Path, error) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin(fail_write=error)
    task = _stub_task(tmp_path, stdin)
    task.inbox_queue.extend([inbox.make_message("a", None), inbox.make_message("b", None)])

    await asyncio.wait_for(registry._pump(task), 5)

    assert task.input_closed and task.inbox_closed
    undelivered = [e["text"] for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert undelivered == ["a", "b"]


@pytest.mark.parametrize("error", [BrokenPipeError(), ConnectionResetError()])
async def test_a_broken_pipe_on_drain_stops_the_pump(tmp_path: Path, error) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin(fail_drain=error)
    task = _stub_task(tmp_path, stdin)
    task.inbox_queue.append(inbox.make_message("a", None))

    await asyncio.wait_for(registry._pump(task), 5)

    assert task.input_closed and task.inbox_closed and stdin.closed
    kinds = [e["kind"] for e in _events(tmp_path, task.task_id)]
    assert kinds.count("user_message") == 1  # written; the pipe broke only afterwards


async def test_a_closing_transport_counts_as_closed(tmp_path: Path) -> None:
    stdin = _Stdin()
    stdin.closed = True
    task = _stub_task(tmp_path, stdin)
    assert tasks_module._write_stdin(task, b"x") is False
    assert task.input_closed


# send refusals on the non-owner path, in order.


def _record(tmp_path: Path, **overrides) -> None:
    fields = dict(
        task_id="t1",
        backend="claude",
        session_id="s",
        repo_path=str(tmp_path),
        started_at=datetime.now(timezone.utc).isoformat(),
        owner=identity.own_identity(),
        live_input=True,
    )
    fields.update(overrides)
    store.write(tmp_path, store.TaskRecord(**fields))


@pytest.mark.parametrize(
    ("overrides", "closed", "code"),
    [
        ({"live_input": False}, True, "not_live_input"),
        ({}, True, "closed"),
        ({"status": "completed", "exit_code": 0}, False, "settled"),
        ({"owner": {"pid": 999_999_9, "start_time": "Mon Jan  1 00:00:00 2001", "markers": []}},
         False, "owner_not_alive"),
        ({"owner": None}, False, "owner_not_alive"),
        ({"owner": {"pid": 1, "start_time": None, "markers": []}}, False, "owner_not_alive"),
    ],
    ids=["not-live", "closed", "settled", "owner-dead", "owner-unknown", "owner-legacy"],
)
def test_a_non_owner_send_is_refused_in_order(tmp_path: Path, overrides, closed, code) -> None:
    _record(tmp_path, **overrides)
    if closed:
        inbox.mark_closed(tmp_path, "t1")
    with pytest.raises(inbox.SendRefused) as caught:
        inbox.send_to_record(tmp_path, "t1", "hi", by=None)
    assert caught.value.code == code
    assert not inbox.inbox_path(tmp_path, "t1").read_bytes()


def test_a_non_owner_send_to_an_unknown_task(tmp_path: Path) -> None:
    with pytest.raises(inbox.SendRefused) as caught:
        inbox.send_to_record(tmp_path, "nope", "hi", by=None)
    assert caught.value.code == "unknown_task"


def test_a_non_owner_send_appends_one_durable_line(tmp_path: Path) -> None:
    _record(tmp_path)
    result = inbox.send_to_record(tmp_path, "t1", "line one\nline two", by={"pid": 7})
    assert result["status"] == "queued"
    messages, offset = inbox.read_new(tmp_path, "t1", 0)
    assert [m["text"] for m in messages] == ["line one\nline two"]
    assert messages[0]["id"] == result["message_id"] and messages[0]["by"] == {"pid": 7}
    assert offset == inbox.inbox_path(tmp_path, "t1").stat().st_size
    assert inbox.read_new(tmp_path, "t1", offset) == ([], offset)


def test_read_new_leaves_a_torn_line_unread(tmp_path: Path) -> None:
    path = inbox.inbox_path(tmp_path, "t1")
    good = json.dumps(inbox.make_message("ok", None)) + "\n"
    path.write_text(good + "not json\n" + '{"id": "x", "text": "partial')
    messages, offset = inbox.read_new(tmp_path, "t1", 0)
    assert [m["text"] for m in messages] == ["ok"]
    assert offset == len((good + "not json\n").encode())


def test_mark_closed_is_idempotent(tmp_path: Path) -> None:
    inbox.mark_closed(tmp_path, "t1")
    inbox.mark_closed(tmp_path, "t1")
    assert inbox.is_closed(tmp_path, "t1")


def test_a_held_inbox_lock_times_out_the_sender(tmp_path: Path) -> None:
    _record(tmp_path)
    fd = inbox.lock_sync(tmp_path, "t1")
    try:
        with pytest.raises(inbox.SendRefused) as caught:
            inbox.send_to_record(tmp_path, "t1", "hi", by=None, timeout=0.2)
        assert caught.value.code == "lock_timeout"
    finally:
        inbox.unlock(fd)


# --- Codex review round 1: regressions -----------------------------------------------------------


def test_background_subagent_events_after_a_result_do_not_reopen_the_turn() -> None:
    """A background subagent's own events carry `parent_tool_use_id`. Treating them as main-turn
    activity kept a stalled one from ever reaching the idle bound."""
    from polybridge.tasks import background_idle_bound_reached as reached

    subagent = {
        "type": "assistant",
        "parent_tool_use_id": "toolu_agent",
        "message": {"content": [{"type": "text", "text": "sub"}]},
    }
    subagent_tool = {**TOOL_RESULT, "parent_tool_use_id": "toolu_agent"}
    acc = _feed(_started("agent", True), _result("STARTED"), subagent, subagent_tool)
    assert not acc.turn_open
    assert acc.background_open == {"agent"}
    assert reached(acc, silent_for=600, bound=600)
    # And once it finishes, the run is idle.
    backends.ClaudeBackend().ingest(_notification("agent", "completed"), acc)
    assert acc.awaiting_input


def test_a_later_turn_that_never_finished_is_not_a_completion() -> None:
    claude = backends.ClaudeBackend()
    acc = _feed(ASSISTANT, _result("first"), ASSISTANT)
    assert claude.classify(acc, None) == "failed"
    assert claude.classify(acc, 0) == "failed"
    acc = _feed(ASSISTANT, _result("first"), ASSISTANT, _result("second"))
    assert claude.classify(acc, None) == "completed"


def test_open_background_work_at_an_unobserved_exit_is_not_a_completion() -> None:
    claude = backends.ClaudeBackend()
    acc = _feed(_started("bg", True), _result("STARTED"))
    assert claude.classify(acc, None) == "failed"
    assert claude.classify(acc, 0) == "completed"  # observed: claude reports its kills itself


def test_a_torn_inbox_line_does_not_swallow_the_next_message(tmp_path: Path) -> None:
    _record(tmp_path)
    path = inbox.inbox_path(tmp_path, "t1")
    path.write_bytes(b'{"id": "crashed", "text": "half a mess')  # a writer died mid-line
    result = inbox.send_to_record(tmp_path, "t1", "after the crash", by=None)
    messages, _ = inbox.read_new(tmp_path, "t1", 0)
    assert [m["id"] for m in messages] == [result["message_id"]]


async def test_an_error_arriving_while_waiting_for_the_lock_stops_forwarding(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count = 1
    task.inbox_queue.append(inbox.make_message("must not run", None))

    fd = inbox.lock_sync(tmp_path, task.task_id)
    pump = asyncio.create_task(registry._pump(task))
    await asyncio.sleep(0.2)  # the pump is waiting for the lock to forward
    task.acc.error_result_seen = True
    inbox.unlock(fd)
    await asyncio.wait_for(pump, 5)

    assert stdin.written == []
    undelivered = [e for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert [e["text"] for e in undelivered] == ["must not run"]
    assert "reported an error" in undelivered[0]["reason"]


async def test_a_marker_that_cannot_be_created_falls_back_to_a_seal_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2: the inbox itself carries the seal when the directory refuses the marker, and
    every sender honours it."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True

    def no_marker(log_dir, task_id):
        raise OSError("read-only directory")

    monkeypatch.setattr(inbox, "mark_closed", no_marker)
    await asyncio.wait_for(registry._pump(task), 5)

    assert stdin.closed and task.inbox_closed
    assert not inbox.closed_path(tmp_path, task.task_id).exists()
    assert inbox.is_closed(tmp_path, task.task_id)  # via the seal line
    _record(tmp_path, task_id=task.task_id)
    with pytest.raises(inbox.SendRefused) as caught:
        inbox.send_to_record(tmp_path, task.task_id, "hi", by=None)
    assert caught.value.code == "closed"
    assert inbox.read_new(tmp_path, task.task_id, 0)[0] == []  # the seal is not a message


async def test_a_close_that_cannot_seal_keeps_input_open_and_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.05)
    real_seal = inbox.seal
    failures = {"left": 3}

    def flaky_seal(log_dir, task_id):
        if failures["left"]:
            failures["left"] -= 1
            raise OSError("disk full")
        real_seal(log_dir, task_id)

    monkeypatch.setattr(inbox, "seal", flaky_seal)
    pump = asyncio.create_task(registry._pump(task))
    await asyncio.sleep(0.05)
    assert not stdin.closed and not task.inbox_closed  # no seal, so input is still open
    await asyncio.wait_for(pump, 5)
    assert failures["left"] == 0
    assert stdin.closed and task.inbox_closed and inbox.is_closed(tmp_path, task.task_id)


async def test_a_close_that_can_never_seal_ends_input_after_the_retry_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2: a permanent failure must not keep a finished run from ever reaching EOF."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.01)
    attempts = {"n": 0}

    def never(log_dir, task_id):
        attempts["n"] += 1
        raise OSError("disk full")

    monkeypatch.setattr(inbox, "seal", never)
    await asyncio.wait_for(registry._pump(task), 5)

    # The retry limit, plus the forced close's own last attempt to seal.
    assert attempts["n"] == tasks_module.CLOSE_RETRY_LIMIT + 1
    assert stdin.closed and task.inbox_closed
    assert any("without the inbox protocol" in n for n in task.bridge_notices)


async def test_an_idle_close_whose_idleness_passed_while_waiting_is_abandoned(
    tmp_path: Path,
) -> None:
    """Round 2: the idle decision is re-made under the lock."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True

    fd = inbox.lock_sync(tmp_path, task.task_id)
    pump = asyncio.create_task(registry._pump(task))
    await asyncio.sleep(0.2)  # the pump decided to close and is waiting for the lock
    backends.ClaudeBackend().ingest(ASSISTANT, task.acc)  # a follow-up turn starts meanwhile
    inbox.unlock(fd)
    await asyncio.sleep(0.3)
    assert not stdin.closed and not task.inbox_closed and not pump.done()

    backends.ClaudeBackend().ingest(_result("follow-up done"), task.acc)
    task.pump_wake.set()
    await asyncio.wait_for(pump, 5)
    assert stdin.closed


async def test_the_idle_bound_is_only_marked_abandoned_when_the_close_happens(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 2: an idle-bound close that could not complete abandons nothing."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    claude = backends.ClaudeBackend()
    for event in (_started("bg", True), _result("STARTED")):
        claude.ingest(event, task.acc)
    task.last_output_at -= 10_000
    monkeypatch.setenv("PB_LIVE_IDLE_SECONDS", "1")
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.05)
    real_seal = inbox.seal
    failures = {"left": 2}

    def flaky_seal(log_dir, task_id):
        if failures["left"]:
            failures["left"] -= 1
            raise OSError("disk full")
        real_seal(log_dir, task_id)

    monkeypatch.setattr(inbox, "seal", flaky_seal)
    pump = asyncio.create_task(registry._pump(task))
    await _until(lambda: failures["left"] == 0)
    assert not task.acc.background_abandoned  # the failed attempts committed nothing
    # The background task finishes meanwhile: nothing left to abandon.
    claude.ingest(_updated("bg", "completed"), task.acc)
    task.pump_wake.set()
    await asyncio.wait_for(pump, 5)
    assert stdin.closed
    assert not task.acc.background_abandoned
    assert claude.classify(task.acc, 0) == "completed"


def test_a_send_to_a_task_whose_process_has_exited_is_refused(tmp_path: Path) -> None:
    """Round 2: the record may still say running, but nothing is left to deliver to."""
    import subprocess

    gone = subprocess.Popen(["/usr/bin/true"])
    gone.wait()  # a real pid, reaped: `ps` finds nothing, which is `dead`, not undecidable
    _record(tmp_path, pid=gone.pid, start_time="Mon Jan  1 00:00:00 2001", markers=["true"])
    with pytest.raises(inbox.SendRefused) as caught:
        inbox.send_to_record(tmp_path, "t1", "hi", by=None)
    assert caught.value.code == "exited"


def test_recovery_knows_a_written_message_was_still_owed_a_turn(tmp_path: Path) -> None:
    """Round 2: the bridge's writes are not in the agent's stdout, so the record carries them."""
    raw = [
        {"type": "system", "subtype": "init", "session_id": "s"},
        ASSISTANT,
        _result("first answer"),
    ]
    store.log_path(tmp_path, "t1").write_text("".join(json.dumps(e) + "\n" for e in raw))
    import subprocess

    gone = subprocess.Popen(["/usr/bin/true"])
    gone.wait()  # its process is gone, so the record resolves from the replay
    dead = {"pid": gone.pid, "start_time": "Mon Jan  1 00:00:00 2001", "markers": ["true"]}
    _record(tmp_path, **dead, input_after_result=1)
    status, _note, state, _tail = store.resolve_status(tmp_path, store.read(tmp_path, "t1"))
    assert status == "failed"
    assert state.turn_open

    _record(tmp_path, **dead, input_after_result=0)
    status, *_ = store.resolve_status(tmp_path, store.read(tmp_path, "t1"))
    assert status == "completed"  # that message was answered by the result in the stream


async def test_writing_a_message_records_that_a_turn_is_owed(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = _stub_task(tmp_path, _Stdin())
    task.acc.result_count, task.acc.awaiting_input = 2, False
    task.inbox_queue.append(inbox.make_message("more", None))
    assert await registry._forward_pending(task)
    assert task.input_after_result == 2
    assert task.acc.turn_open and not task.acc.awaiting_input
    assert store.read(tmp_path, task.task_id).input_after_result == 2


async def test_an_unreadable_inbox_loses_nothing_while_the_run_lives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.inbox_queue.append(inbox.make_message("kept", None))
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.05)
    real_read = inbox.read_new
    failures = {"left": 2}

    def flaky_read(*args):
        if failures["left"]:
            failures["left"] -= 1
            raise OSError("EIO")
        return real_read(*args)

    monkeypatch.setattr(inbox, "read_new", flaky_read)
    pump = asyncio.create_task(registry._pump(task))
    await _until(lambda: stdin.written)
    assert _texts(stdin.written) == ["kept"]
    task.acc.result_count, task.acc.awaiting_input = 1, True
    task.pump_wake.set()
    await asyncio.wait_for(pump, 5)
    assert stdin.closed


async def test_after_exit_a_lock_that_stays_busy_falls_back_without_losing_messages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The last-resort close reads the on-disk inbox too, not only this server's queue."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.proc.returncode = 3
    task.inbox_queue.append(inbox.make_message("in memory", None))
    fd = inbox.lock_sync(tmp_path, task.task_id)
    inbox.append_locked(tmp_path, task.task_id, inbox.make_message("on disk", None))

    real_lock = inbox.lock_async

    async def quick_lock(log_dir, task_id, timeout=inbox.LOCK_TIMEOUT_SECONDS):
        return await real_lock(log_dir, task_id, timeout=0.1)

    monkeypatch.setattr(inbox, "lock_async", quick_lock)
    monkeypatch.setattr(tasks_module, "EXIT_CLOSE_GIVE_UP_SECONDS", 0.3)
    try:
        await asyncio.wait_for(registry._pump(task), 5)
    finally:
        inbox.unlock(fd)

    assert task.inbox_closed and inbox.is_closed(tmp_path, task.task_id)
    undelivered = sorted(
        e["text"] for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"
    )
    assert undelivered == ["in memory", "on disk"]


async def test_finish_pump_fallback_reads_the_disk_inbox(tmp_path: Path) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = _stub_task(tmp_path, _Stdin())
    task.proc.returncode = 0
    fd = inbox.lock_sync(tmp_path, task.task_id)
    inbox.append_locked(tmp_path, task.task_id, inbox.make_message("on disk", None))
    inbox.unlock(fd)
    # No pump at all (it failed to start, say): the monitor's fallback still accounts for it.
    await registry._finish_pump(task)
    undelivered = [e["text"] for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert undelivered == ["on disk"]
    assert inbox.is_closed(tmp_path, task.task_id)


def test_a_sender_that_finds_the_marker_after_appending_is_refused(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The last-resort close creates the marker without the lock, then reads: a sender whose
    append raced it must not report "queued" for a message that read may have missed."""
    _record(tmp_path)
    real_append = inbox.append_locked

    def append_then_close(log_dir, task_id, message):
        real_append(log_dir, task_id, message)
        inbox.mark_closed(log_dir, task_id)  # the unlocked close lands right after the append

    monkeypatch.setattr(inbox, "append_locked", append_then_close)
    with pytest.raises(inbox.SendRefused) as caught:
        inbox.send_to_record(tmp_path, "t1", "hi", by=None)
    assert caught.value.code == "closed"


# --- Codex review round 3: regressions -----------------------------------------------------------


async def test_a_forced_close_with_background_work_open_is_abandonment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """EOF from a forced close kills open background work just as the idle bound's does."""
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    claude = backends.ClaudeBackend()
    for event in (_started("bg", True), _result("STARTED")):
        claude.ingest(event, task.acc)
    task.last_output_at -= 10_000
    monkeypatch.setenv("PB_LIVE_IDLE_SECONDS", "1")
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.01)

    def never(log_dir, task_id):
        raise OSError("disk full")

    monkeypatch.setattr(inbox, "seal", never)
    await asyncio.wait_for(registry._pump(task), 5)

    assert stdin.closed
    assert task.acc.background_abandoned
    claude.ingest(_updated("bg", "killed"), task.acc)  # what claude reports after that EOF
    assert claude.classify(task.acc, 0) == "failed"


async def test_a_forced_close_that_could_not_read_the_inbox_is_reconciled_before_finishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.acc.result_count, task.acc.awaiting_input = 1, True
    fd = inbox.lock_sync(tmp_path, task.task_id)
    inbox.append_locked(tmp_path, task.task_id, inbox.make_message("acknowledged on disk", None))
    inbox.unlock(fd)
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.01)
    real_read = inbox.read_new
    broken = {"on": True}

    def read(*args):
        if broken["on"]:
            raise OSError("EIO")
        return real_read(*args)

    monkeypatch.setattr(inbox, "read_new", read)
    await asyncio.wait_for(registry._pump(task), 5)
    assert stdin.closed and task.inbox_closed and not task.inbox_reconciled
    assert inbox.is_closed(tmp_path, task.task_id)  # the forced close still sealed it

    broken["on"] = False  # the disk recovers before the run is published
    task.proc.returncode = 0
    await registry._finish_pump(task)
    assert task.inbox_reconciled
    undelivered = [e["text"] for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert undelivered == ["acknowledged on disk"]


# --- Independent verifier, follow-up round: regressions -------------------------------------------


async def test_a_message_appended_after_a_forced_close_that_could_not_seal_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verifier #2: the forced close read the inbox fine but could not seal it. Once its lock was
    released another process saw no marker, appended, and was told "queued" — and `_finish_pump`
    skipped the reread because the inbox counted as reconciled. It is reconciled only once sealed."""
    owner = identity.own_identity()
    registry = TaskRegistry(log_dir=tmp_path, owner=owner)
    stdin = _Stdin()
    task = _stub_task(tmp_path, stdin)
    task.owner = owner
    task.acc.result_count, task.acc.awaiting_input = 1, True
    _record(tmp_path, task_id=task.task_id, owner=owner)
    monkeypatch.setattr(tasks_module, "PUMP_POLL_SECONDS", 0.01)

    def never(log_dir, task_id):
        raise OSError("disk full")

    monkeypatch.setattr(inbox, "seal", never)
    monkeypatch.setattr(identity, "identity_check", lambda ident: "alive")
    await asyncio.wait_for(registry._pump(task), 5)
    assert stdin.closed and task.inbox_closed
    assert not task.inbox_sealed and not task.inbox_reconciled

    queued = inbox.send_to_record(tmp_path, task.task_id, "sent after the forced close", by=None)
    assert queued["status"] == "queued"  # the unsealed inbox still looks open to a sender

    task.proc.returncode = 0
    await registry._finish_pump(task)

    assert task.inbox_reconciled
    undelivered = [e for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert [e["message_id"] for e in undelivered] == [queued["message_id"]]
    assert any("was not delivered" in n for n in task.bridge_notices)


async def test_an_exit_path_close_that_could_not_seal_still_gets_the_final_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = _stub_task(tmp_path, _Stdin())
    task.proc.returncode = 0

    def never(log_dir, task_id):
        raise OSError("disk full")

    monkeypatch.setattr(inbox, "seal", never)
    await asyncio.wait_for(registry._pump(task), 5)
    assert task.inbox_closed and not task.inbox_reconciled

    fd = inbox.lock_sync(tmp_path, task.task_id)
    inbox.append_locked(tmp_path, task.task_id, inbox.make_message("late", None))
    inbox.unlock(fd)
    await registry._finish_pump(task)
    undelivered = [e["text"] for e in _events(tmp_path, task.task_id) if e["kind"] == "undelivered"]
    assert undelivered == ["late"]


async def test_a_sealed_close_needs_no_final_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry = TaskRegistry(log_dir=tmp_path, owner=OWNER)
    task = _stub_task(tmp_path, _Stdin())
    task.acc.result_count, task.acc.awaiting_input = 1, True
    await asyncio.wait_for(registry._pump(task), 5)
    assert task.inbox_sealed and task.inbox_reconciled
    calls = []
    monkeypatch.setattr(registry, "_final_inbox_read", lambda t: calls.append(t))
    task.proc.returncode = 0
    await registry._finish_pump(task)
    assert calls == []
