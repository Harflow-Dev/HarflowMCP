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
