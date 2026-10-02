"""The normalized event log a task writes alongside its raw stream — `<task_id>.events.jsonl`."""

from __future__ import annotations

import asyncio
import json
import signal
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from polybridge import backends, tasks as tasks_module
from polybridge.backends import Invocation
from polybridge.backends.base import Enforcement
from polybridge.events import EventLog, events_path
from polybridge.tasks import Task, TaskRegistry


class _RecordingBackend:
    """A double that spawns a real, instant `/bin/sh` process emitting a fixed line of JSON per
    stdout line, so the drain path can be exercised end to end without a real agent CLI."""

    name = "recording-double"
    binary = "/bin/sh"
    capabilities = SimpleNamespace(chooses_session_id=False, supports_live_input=False)

    def __init__(self, script: str, normalize_fn=None, classify_result: str = "completed") -> None:
        self.script = script
        self._normalize_fn = normalize_fn or (lambda event, acc: [])
        self._classify_result = classify_result

    def build_start_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", self.script])

    def build_resume_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", self.script])

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)
        return None

    def encode_live_message(self, text):
        raise backends.UnsupportedCapability("no live input")

    def interactive_resume_argv(self, session_id, repo_path):
        return [self.binary, "--resume", session_id]

    def enforcement(self, freedom, network=None):
        return Enforcement(
            freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False
        )

    def ingest(self, event, acc):
        return None

    def normalize(self, event, acc):
        return self._normalize_fn(event, acc)

    def classify(self, acc, exit_code):
        return self._classify_result


@pytest.fixture
def registered(monkeypatch: pytest.MonkeyPatch):
    """Register a `_RecordingBackend` under `backends.BACKENDS` so `get_backend` (used by the
    drainer, keyed by name rather than by the object `_spawn` was handed) can find it."""

    def _register(backend: _RecordingBackend) -> _RecordingBackend:
        monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
        return backend

    return _register


def _two_line_script() -> str:
    return "printf '%s\\n' '{\"type\":\"x\"}' '{\"type\":\"y\"}'"


async def test_seq_zero_is_always_task_started_and_seqs_are_contiguous(
    tmp_path: Path, registered
) -> None:
    backend = registered(
        _RecordingBackend(
            _two_line_script(),
            normalize_fn=lambda event, acc: [{"kind": "notice", "text": event.get("type")}],
        )
    )
    registry = TaskRegistry(log_dir=tmp_path, owner={"pid": 1, "start_time": "t", "markers": []})

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    lines = [
        json.loads(line)
        for line in events_path(tmp_path, task.task_id).read_text().splitlines()
    ]
    assert lines[0]["kind"] == "task_started"
    assert lines[0]["seq"] == 0
    assert [line["seq"] for line in lines] == list(range(len(lines)))
    assert lines[-1]["kind"] == "task_finished"


async def test_raw_offset_matches_the_raw_logs_byte_length_through_that_line(
    tmp_path: Path, registered
) -> None:
    backend = registered(
        _RecordingBackend(
            _two_line_script(),
            normalize_fn=lambda event, acc: [{"kind": "notice", "text": event.get("type")}],
        )
    )
    registry = TaskRegistry(log_dir=tmp_path)

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    raw_bytes = task.log_path.read_bytes()
    raw_lines = raw_bytes.splitlines(keepends=True)
    assert len(raw_lines) == 2
    expected_offsets = [len(raw_lines[0]), len(raw_lines[0]) + len(raw_lines[1])]

    notices = [
        json.loads(line)
        for line in events_path(tmp_path, task.task_id).read_text().splitlines()
        if json.loads(line)["kind"] == "notice"
    ]
    assert [n["raw_offset"] for n in notices] == expected_offsets


async def test_raw_offset_is_null_when_the_raw_log_cannot_be_opened(
    tmp_path: Path, registered, monkeypatch: pytest.MonkeyPatch
) -> None:
    backend = registered(
        _RecordingBackend(
            _two_line_script(),
            normalize_fn=lambda event, acc: [{"kind": "notice", "text": event.get("type")}],
        )
    )
    registry = TaskRegistry(log_dir=tmp_path)

    # Pre-create the raw log path *as a directory* so `Path.open("ab")` fails with an OSError —
    # the same failure mode the drainer already tolerates for a broken disk. `uuid4` is patched to
    # a fixed value so the task's log path is known ahead of spawning it.
    fixed_id = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setattr(tasks_module.uuid, "uuid4", lambda: fixed_id)
    (tmp_path / f"{fixed_id}.jsonl").mkdir()

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    notices = [
        json.loads(line)
        for line in events_path(tmp_path, task.task_id).read_text().splitlines()
        if json.loads(line)["kind"] == "notice"
    ]
    assert notices, "the normalized events must still be recorded despite the raw log failing"
    assert all(n["raw_offset"] is None for n in notices)


async def test_raw_offset_is_null_after_the_raw_log_handle_is_dropped(
    tmp_path: Path, registered, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write failure on the raw log mid-stream disables it for the rest of the run."""
    backend = registered(
        _RecordingBackend(
            _two_line_script(),
            normalize_fn=lambda event, acc: [{"kind": "notice", "text": event.get("type")}],
        )
    )
    registry = TaskRegistry(log_dir=tmp_path)

    class _FlakyHandle:
        def __init__(self, real):
            self._real = real
            self._writes = 0

        def write(self, data):
            self._writes += 1
            if self._writes > 1:
                raise OSError("disk full")
            return self._real.write(data)

        def flush(self):
            self._real.flush()

        def tell(self):
            return self._real.tell()

        def close(self):
            self._real.close()

    real_open = Path.open

    def flaky_open(self, mode="r", *args, **kwargs):
        handle = real_open(self, mode, *args, **kwargs)
        if mode == "ab" and self.name.endswith(".jsonl") and ".events." not in self.name:
            return _FlakyHandle(handle)
        return handle

    monkeypatch.setattr(Path, "open", flaky_open)

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    notices = [
        json.loads(line)
        for line in events_path(tmp_path, task.task_id).read_text().splitlines()
        if json.loads(line)["kind"] == "notice"
    ]
    assert len(notices) == 2
    assert notices[0]["raw_offset"] is not None
    assert notices[1]["raw_offset"] is None


async def test_a_raising_normalizer_bumps_normalize_errors_and_never_touches_status(
    tmp_path: Path, registered
) -> None:
    def boom(event, acc):
        raise RuntimeError("normalizer bug")

    backend = registered(
        _RecordingBackend(_two_line_script(), normalize_fn=boom, classify_result="completed")
    )
    registry = TaskRegistry(log_dir=tmp_path)

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    assert task.acc.normalize_errors == 2
    assert task.status == "completed"
    assert not task.drain_failed


async def test_no_task_finished_event_on_the_abandoned_path(tmp_path: Path) -> None:
    """A torn-down server's monitor must not write `task_finished` for a run it never observed
    the end of — mirrors `test_a_cancelled_monitor_leaves_a_live_run_recorded_as_running` in
    test_registry.py, plus the events-log assertion that test does not need."""
    registry = TaskRegistry(log_dir=tmp_path)
    proc = await asyncio.create_subprocess_exec(
        "sleep",
        "30",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    task = Task(
        task_id="abandoned-task",
        backend="claude",
        session_id="s",
        repo_path=tmp_path,
        prompt="x",
        max_turns=5,
        log_path=tmp_path / "abandoned-task.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    task.proc = proc
    task.pgid = proc.pid
    task.events = EventLog(events_path(tmp_path, task.task_id), task.task_id)
    task.events.write("task_started", {"backend": "claude"})
    registry._tasks[task.task_id] = task

    monitor = asyncio.create_task(tasks_module._monitor(task, registry))
    await asyncio.sleep(0.05)
    monitor.cancel()
    await asyncio.gather(monitor, return_exceptions=True)

    assert task.status == "running"
    kinds = [
        json.loads(line)["kind"]
        for line in events_path(tmp_path, task.task_id).read_text().splitlines()
    ]
    assert "task_finished" not in kinds

    tasks_module._signal_group(task, signal.SIGKILL)
    await proc.wait()
    task.events.close()


async def test_exactly_one_task_finished_event_on_the_normal_path(
    tmp_path: Path, registered
) -> None:
    backend = registered(_RecordingBackend(_two_line_script()))
    registry = TaskRegistry(log_dir=tmp_path)

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    kinds = [
        json.loads(line)["kind"]
        for line in events_path(tmp_path, task.task_id).read_text().splitlines()
    ]
    assert kinds.count("task_finished") == 1
    finished = json.loads(
        [
            line
            for line in events_path(tmp_path, task.task_id).read_text().splitlines()
            if json.loads(line)["kind"] == "task_finished"
        ][0]
    )
    assert finished["status"] == task.status
    assert finished["exit_code"] == task.exit_code
    assert finished["observed"] is True


# --- git baseline ---------------------------------------------------------------------------


async def test_git_baseline_succeeds_in_a_clean_repo(git_repo: Path, registered) -> None:
    subprocess.run(["git", "-C", str(git_repo), "commit", "-qm", "x", "--allow-empty"], check=True)
    backend = registered(_RecordingBackend(_two_line_script()))
    registry = TaskRegistry(log_dir=git_repo / ".streams")

    task = await registry.start("hi", git_repo, backend=backend)
    await task.done.wait()

    assert task.base_commit is not None
    assert task.start_dirty is None


async def test_git_baseline_does_not_probe_a_dirty_worktree(git_repo: Path, registered) -> None:
    """Telling dirty from clean needs `git status`, which runs the repository's own filters in the
    server process — so `start_dirty` is never probed (see `tasks._git_baseline`)."""
    subprocess.run(["git", "-C", str(git_repo), "commit", "-qm", "x", "--allow-empty"], check=True)
    (git_repo / "untracked.txt").write_text("hi")
    backend = registered(_RecordingBackend(_two_line_script()))
    registry = TaskRegistry(log_dir=git_repo / ".streams")

    task = await registry.start("hi", git_repo, backend=backend)
    await task.done.wait()

    assert task.base_commit is not None
    assert task.start_dirty is None


async def test_git_baseline_is_null_outside_a_repo(tmp_path: Path, registered) -> None:
    backend = registered(_RecordingBackend(_two_line_script()))
    registry = TaskRegistry(log_dir=tmp_path / ".streams")

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    assert task.base_commit is None
    assert task.start_dirty is None


async def test_a_hanging_git_baseline_is_cut_off_by_its_own_budget(
    git_repo: Path, registered, monkeypatch: pytest.MonkeyPatch
) -> None:
    subprocess.run(["git", "-C", str(git_repo), "commit", "-qm", "x", "--allow-empty"], check=True)
    monkeypatch.setattr(tasks_module, "GIT_BASELINE_BUDGET_SECONDS", 0.05)

    def hang(repo_path):
        import time

        time.sleep(5)
        return "should-not-be-seen", False

    monkeypatch.setattr(tasks_module, "_git_baseline", hang)
    backend = registered(_RecordingBackend(_two_line_script()))
    registry = TaskRegistry(log_dir=git_repo / ".streams")

    task = await asyncio.wait_for(
        registry.start("hi", git_repo, backend=backend), timeout=2
    )
    await task.done.wait()

    assert task.base_commit is None
    assert task.start_dirty is None


# --- owner / start_time -----------------------------------------------------------------------


async def test_owner_and_start_time_land_in_the_persisted_record(
    tmp_path: Path, registered
) -> None:
    from polybridge import store

    owner = {"pid": 4242, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}
    # A brief sleep so the process is still alive when `_spawn` captures its own start time —
    # the plain two-line script can exit and be reaped before that capture runs.
    backend = registered(_RecordingBackend("printf '%s\\n' '{\"type\":\"x\"}'; sleep 0.3"))
    registry = TaskRegistry(log_dir=tmp_path, owner=owner)

    task = await registry.start("hi", tmp_path, backend=backend)
    await task.done.wait()

    assert task.owner == owner
    assert task.start_time is not None

    record = store.read(tmp_path, task.task_id)
    assert record is not None
    assert record.owner == owner
    assert record.start_time == task.start_time


def test_envelope_keys_lead_each_line_and_win_over_colliding_fields(tmp_path: Path) -> None:
    log = EventLog(tmp_path / "t.events.jsonl", "t")
    log.write("notice", {"text": "hi", "seq": 99, "kind": "forged"}, raw_offset=7)
    log.close()

    entry = json.loads((tmp_path / "t.events.jsonl").read_text())
    assert list(entry)[:7] == ["v", "seq", "observed_at", "source_ts", "raw_offset", "task_id", "kind"]
    assert (entry["seq"], entry["kind"], entry["raw_offset"], entry["text"]) == (0, "notice", 7, "hi")


def test_every_line_is_schema_version_one(tmp_path: Path) -> None:
    """The Monitor app reads this file. Changing the envelope must bump `v`, not drift under it."""
    log = EventLog(tmp_path / "t.events.jsonl", "t")
    log.write("notice", {"text": "a"})
    log.write("notice", {"text": "b", "v": 2})
    log.close()

    lines = [json.loads(line) for line in (tmp_path / "t.events.jsonl").read_text().splitlines()]
    assert [line["v"] for line in lines] == [1, 1]


def test_an_unserializable_event_is_dropped_without_disabling_the_log(tmp_path: Path) -> None:
    circular: dict = {}
    circular["self"] = circular
    log = EventLog(tmp_path / "t.events.jsonl", "t")

    assert log.write("notice", {"bad": circular}) is False
    assert log.write("notice", {"text": "next"}) is True
    log.close()

    lines = [json.loads(line) for line in (tmp_path / "t.events.jsonl").read_text().splitlines()]
    assert [(line["seq"], line["text"]) for line in lines] == [(0, "next")]


# --- the closed v1 kind set (Stage C switches on it) ----------------------------------------

import re as _re  # noqa: E402

from polybridge import events as events_module  # noqa: E402
from polybridge.backends import normalize as nz  # noqa: E402

_SRC = Path(__file__).resolve().parent.parent / "src" / "polybridge"


@pytest.mark.allow_unknown_event_kinds
def test_writing_an_unknown_kind_raises_and_writes_nothing(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    log = EventLog(path, "t")

    with pytest.raises(events_module.UnknownEventKind):
        log.write("assistant_thought", {"text": "x"})
    assert log.write("notice", {"text": "still usable"}) is True
    log.close()

    lines = [json.loads(line) for line in path.read_text().splitlines()]
    assert [(e["seq"], e["kind"]) for e in lines] == [(0, "notice")]


def test_every_normalize_helper_builds_a_listed_kind() -> None:
    acc = backends.Accumulator()
    built = [
        nz.tool_call(call_id="c1", tool="Bash", category="shell", input={"command": "ls"}),
        nz.tool_result(call_id="c1", ok=True, output="ok"),
        nz.assistant_text("hi"),
        nz.assistant_delta("hi", message_id="m", block_index=1),
        nz.user_message("hi", "initial"),
        nz.notice("n"),
        nz.usage(acc),
    ]
    assert {event["kind"] for event in built} <= events_module.EVENT_KINDS


def test_assistant_delta_is_a_listed_kind_and_the_log_version_stays_one(tmp_path: Path) -> None:
    """A new event *kind* is additive under the frozen v1 envelope — an older Monitor decodes it
    as `.unknown` rather than rejecting the log — so `assistant_delta` must NOT bump
    `EVENT_LOG_VERSION`, which would make older Monitors reject every new log."""
    path = tmp_path / "t.events.jsonl"
    log = EventLog(path, "t")
    assert log.write("assistant_delta", {"text": "chunk", "message_id": "m", "block_index": 1})
    log.close()

    entry = json.loads(path.read_text())
    assert entry["kind"] == "assistant_delta"
    assert entry["v"] == events_module.EVENT_LOG_VERSION == 1


def _emitted_literal_kinds() -> set[str]:
    """Every kind named literally at an emit site in the package: the bridge's `_write_event(task,
    "<kind>", …)` and `events.write("<kind>", …)`, and the `"kind": "<kind>"` dicts in
    `backends/normalize.py`, which is where every backend's normalized events are built."""
    kinds: set[str] = set()
    for path in _SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        kinds.update(_re.findall(r'_write_event\(\s*\w+,\s*"(\w+)"', text))
        kinds.update(_re.findall(r'events\.write\(\s*"(\w+)"', text))
    kinds.update(_re.findall(r'"kind":\s*"(\w+)"', (_SRC / "backends" / "normalize.py").read_text()))
    return kinds


def test_every_kind_emitted_in_the_code_is_listed_and_every_listed_kind_is_emitted() -> None:
    emitted = _emitted_literal_kinds()
    assert emitted == events_module.EVENT_KINDS, (
        f"emitted but unlisted: {sorted(emitted - events_module.EVENT_KINDS)}; "
        f"listed but never emitted: {sorted(events_module.EVENT_KINDS - emitted)}"
    )


# `"kind"` dicts in backend modules that are not events: vibe records an auto-denied approval as
# `{"kind": "approval"}` on `acc.denials` (permission_denials), never in the events log.
_NON_EVENT_KIND_LITERALS = frozenset({"approval"})


def test_no_backend_names_an_unlisted_kind_outside_the_normalize_helpers() -> None:
    """Backends build events through `backends/normalize.py`, which the scan above reads; any other
    `"kind": "<x>"` literal in a backend module must be a listed kind or a known non-event dict."""
    for path in (_SRC / "backends").glob("*.py"):
        if path.name == "normalize.py":
            continue
        found = set(_re.findall(r'"kind":\s*"(\w+)"', path.read_text()))
        stray = found - events_module.EVENT_KINDS - _NON_EVENT_KIND_LITERALS
        assert not stray, f"{path.name}: {sorted(stray)}"


# --- Tests for bounded readers (read_recent, read_page) and formatter -------------------------


def _write_events_file(path: Path, lines: list[str]) -> None:
    """Helper to write test events to a file."""
    path.write_text("\n".join(lines) + "\n")


class TestReadRecent:
    """Tests for read_recent bounded backward reader."""

    def test_missing_file_returns_empty_list(self, tmp_path: Path) -> None:
        """Missing or unreadable file → []."""
        result = events_module.read_recent(tmp_path / "nope.events.jsonl")
        assert result == []

    def test_empty_file_returns_empty_list(self, tmp_path: Path) -> None:
        """Empty file → []."""
        path = tmp_path / "empty.events.jsonl"
        path.write_text("")
        result = events_module.read_recent(path)
        assert result == []

    def test_reads_meaningful_events_from_simple_file(self, tmp_path: Path) -> None:
        """Basic read of meaningful events."""
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps(
                    {
                        "v": 1,
                        "seq": 0,
                        "kind": "task_started",
                        "task_id": "t1",
                        "backend": "claude",
                    }
                ),
                json.dumps(
                    {
                        "v": 1,
                        "seq": 1,
                        "kind": "tool_call",
                        "task_id": "t1",
                        "category": "shell",
                        "tool": "bash",
                        "input_preview": "echo hello",
                    }
                ),
                json.dumps(
                    {
                        "v": 1,
                        "seq": 2,
                        "kind": "assistant_text",
                        "task_id": "t1",
                        "text": "All done",
                    }
                ),
            ],
        )
        result = events_module.read_recent(path)
        assert len(result) == 2  # task_started is filtered out
        assert "shell  bash  echo hello" in result
        assert "text  All done" in result

    def test_limit_respected(self, tmp_path: Path) -> None:
        """Returns at most limit entries."""
        path = tmp_path / "test.events.jsonl"
        lines = []
        for i in range(10):
            lines.append(
                json.dumps(
                    {
                        "v": 1,
                        "seq": i,
                        "kind": "tool_call",
                        "task_id": "t1",
                        "category": "shell",
                        "tool": f"cmd{i}",
                    }
                )
            )
        _write_events_file(path, lines)
        result = events_module.read_recent(path, limit=3)
        assert len(result) == 3

    def test_filters_out_non_meaningful_kinds(self, tmp_path: Path) -> None:
        """Non-meaningful kinds (usage, task_started, task_finished, assistant_delta) excluded."""
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps({"v": 1, "seq": 0, "kind": "task_started", "task_id": "t1"}),
                json.dumps({"v": 1, "seq": 1, "kind": "usage", "task_id": "t1"}),
                json.dumps(
                    {
                        "v": 1,
                        "seq": 2,
                        "kind": "assistant_delta",
                        "task_id": "t1",
                        "text": "chunk",
                    }
                ),
                json.dumps(
                    {
                        "v": 1,
                        "seq": 3,
                        "kind": "assistant_text",
                        "task_id": "t1",
                        "text": "done",
                    }
                ),
                json.dumps(
                    {"v": 1, "seq": 4, "kind": "task_finished", "task_id": "t1"}
                ),
            ],
        )
        result = events_module.read_recent(path)
        assert len(result) == 1
        assert "text  done" in result

    def test_skips_unparsable_lines(self, tmp_path: Path) -> None:
        """Unparsable lines are skipped without error."""
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps(
                    {"v": 1, "seq": 0, "kind": "tool_call", "task_id": "t1", "tool": "a"}
                ),
                "not valid json",
                json.dumps(
                    {"v": 1, "seq": 1, "kind": "tool_call", "task_id": "t1", "tool": "b"}
                ),
            ],
        )
        result = events_module.read_recent(path)
        assert len(result) == 2

    def test_handles_trailing_partial_line(self, tmp_path: Path) -> None:
        """A trailing partial line (no newline) is dropped."""
        path = tmp_path / "test.events.jsonl"
        path.write_text(
            json.dumps({"v": 1, "seq": 0, "kind": "tool_call", "task_id": "t1", "tool": "a"})
            + "\n"  # complete line
            + json.dumps(
                {"v": 1, "seq": 1, "kind": "tool_call", "task_id": "t1", "tool": "b"}
            )[:50]  # partial line
        )
        result = events_module.read_recent(path)
        assert len(result) == 1  # Only the complete line

    def test_truncates_long_lines(self, tmp_path: Path) -> None:
        """Lines longer than 160 chars are truncated with ..."""
        path = tmp_path / "test.events.jsonl"
        long_text = "x" * 200
        _write_events_file(
            path,
            [
                json.dumps(
                    {
                        "v": 1,
                        "seq": 0,
                        "kind": "assistant_text",
                        "task_id": "t1",
                        "text": long_text,
                    }
                ),
            ],
        )
        result = events_module.read_recent(path)
        assert len(result) == 1
        assert len(result[0]) <= 160
        assert result[0].endswith("…")

    def test_failed_tool_result_formatted(self, tmp_path: Path) -> None:
        """Failed tool_result shows as 'failed  <tool>: <first line>'."""
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps(
                    {
                        "v": 1,
                        "seq": 0,
                        "kind": "tool_call",
                        "task_id": "t1",
                        "call_id": "c1",
                        "tool": "Bash",
                    }
                ),
                json.dumps(
                    {
                        "v": 1,
                        "seq": 1,
                        "kind": "tool_result",
                        "task_id": "t1",
                        "call_id": "c1",
                        "ok": False,
                        "output_tail": "Error: command not found",
                    }
                ),
            ],
        )
        result = events_module.read_recent(path)
        # Should have both tool_call and failed tool_result
        assert any("failed  Bash: Error: command not found" in line for line in result)


class TestReadPage:
    """Tests for read_page bounded forward reader."""

    def test_missing_file_returns_empty(self, tmp_path: Path) -> None:
        result = events_module.read_page(tmp_path / "nope.events.jsonl", limit=50)
        assert result.events == []
        assert result.has_more is False
        assert result.skipped_oversized == 0

    def test_empty_file_returns_empty(self, tmp_path: Path) -> None:
        path = tmp_path / "empty.events.jsonl"
        path.write_text("")
        result = events_module.read_page(path, limit=50)
        assert result.events == []
        assert result.has_more is False

    def test_returns_events_in_order(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps({"v": 1, "seq": 0, "kind": "tool_call", "task_id": "t1"}),
                json.dumps({"v": 1, "seq": 1, "kind": "tool_call", "task_id": "t1"}),
                json.dumps({"v": 1, "seq": 2, "kind": "tool_call", "task_id": "t1"}),
            ],
        )
        result = events_module.read_page(path, limit=50)
        assert [e["seq"] for e in result.events] == [0, 1, 2]

    def test_limit_caps_results(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        lines = []
        for i in range(10):
            lines.append(json.dumps({"v": 1, "seq": i, "kind": "tool_call", "task_id": "t1"}))
        _write_events_file(path, lines)
        result = events_module.read_page(path, limit=5)
        assert len(result.events) == 5
        # Default: newest page, so we get the last 5 events (seq 5-9)
        assert [e["seq"] for e in result.events] == [5, 6, 7, 8, 9]

    def test_before_seq_exclusive(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        lines = []
        for i in range(10):
            lines.append(json.dumps({"v": 1, "seq": i, "kind": "tool_call", "task_id": "t1"}))
        _write_events_file(path, lines)
        result = events_module.read_page(path, limit=50, before_seq=5)
        assert [e["seq"] for e in result.events] == [0, 1, 2, 3, 4]

    def test_after_seq_exclusive(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        lines = []
        for i in range(10):
            lines.append(json.dumps({"v": 1, "seq": i, "kind": "tool_call", "task_id": "t1"}))
        _write_events_file(path, lines)
        result = events_module.read_page(path, limit=50, after_seq=5)
        assert [e["seq"] for e in result.events] == [6, 7, 8, 9]

    def test_both_cursors_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        path.write_text("")
        with pytest.raises(ValueError, match="both before_seq and after_seq"):
            events_module.read_page(path, limit=50, before_seq=5, after_seq=3)

    def test_kinds_filter(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps(
                    {"v": 1, "seq": 0, "kind": "tool_call", "task_id": "t1", "tool": "a"}
                ),
                json.dumps(
                    {"v": 1, "seq": 1, "kind": "assistant_text", "task_id": "t1", "text": "hi"}
                ),
                json.dumps(
                    {"v": 1, "seq": 2, "kind": "tool_call", "task_id": "t1", "tool": "b"}
                ),
            ],
        )
        result = events_module.read_page(path, limit=50, kinds=["tool_call"])
        assert len(result.events) == 2
        assert all(e["kind"] == "tool_call" for e in result.events)

    def test_empty_kinds_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        with pytest.raises(ValueError, match="kinds cannot be empty"):
            events_module.read_page(path, limit=50, kinds=[])

    def test_unknown_kind_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        with pytest.raises(ValueError, match="unknown kind"):
            events_module.read_page(path, limit=50, kinds=["unknown_kind"])

    def test_limit_out_of_bounds_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        with pytest.raises(ValueError, match="limit must be 1..200"):
            events_module.read_page(path, limit=0)
        with pytest.raises(ValueError, match="limit must be 1..200"):
            events_module.read_page(path, limit=201)

    def test_excludes_assistant_delta_by_default(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps(
                    {
                        "v": 1,
                        "seq": 0,
                        "kind": "assistant_delta",
                        "task_id": "t1",
                        "text": "chunk",
                    }
                ),
                json.dumps(
                    {"v": 1, "seq": 1, "kind": "assistant_text", "task_id": "t1", "text": "done"}
                ),
            ],
        )
        result = events_module.read_page(path, limit=50)
        assert len(result.events) == 1
        assert result.events[0]["kind"] == "assistant_text"

    def test_includes_assistant_delta_when_requested(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        _write_events_file(
            path,
            [
                json.dumps(
                    {
                        "v": 1,
                        "seq": 0,
                        "kind": "assistant_delta",
                        "task_id": "t1",
                        "text": "chunk",
                    }
                ),
            ],
        )
        result = events_module.read_page(path, limit=50, kinds=["assistant_delta"])
        assert len(result.events) == 1
        assert result.events[0]["kind"] == "assistant_delta"

    def test_cursors_set_correctly(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        lines = []
        for i in range(10):
            lines.append(json.dumps({"v": 1, "seq": i, "kind": "tool_call", "task_id": "t1"}))
        _write_events_file(path, lines)
        result = events_module.read_page(path, limit=5, after_seq=2)
        # Both cursors are set - next_before_seq is oldest in page, next_after_seq is newest
        assert result.next_before_seq == 3  # Oldest in page (seq 3)
        assert result.next_after_seq == 7  # Newest in page (seq 7)

    def test_has_more_set_correctly(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        lines = []
        for i in range(10):
            lines.append(json.dumps({"v": 1, "seq": i, "kind": "tool_call", "task_id": "t1"}))
        _write_events_file(path, lines)
        result = events_module.read_page(path, limit=5)
        assert result.has_more is True
        result2 = events_module.read_page(path, limit=50)
        assert result2.has_more is False

    def test_truncates_long_strings(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        long_text = "x" * 3000
        _write_events_file(
            path,
            [
                json.dumps(
                    {
                        "v": 1,
                        "seq": 0,
                        "kind": "assistant_text",
                        "task_id": "t1",
                        "text": long_text,
                    }
                ),
            ],
        )
        result = events_module.read_page(path, limit=50)
        assert len(result.events) == 1
        assert "truncated" in result.events[0]
        assert len(result.events[0]["text"]) <= 2003  # 2000 + "..."

    def test_skips_oversized_lines(self, tmp_path: Path) -> None:
        path = tmp_path / "test.events.jsonl"
        # Create a line > 1 MiB
        huge_line = json.dumps(
            {"v": 1, "seq": 0, "kind": "tool_call", "task_id": "t1", "tool": "x" * 200000}
        )
        # Make it > 1 MiB
        while len(huge_line.encode("utf-8")) <= 1_000_000:
            huge_line += "x"
        _write_events_file(
            path,
            [
                json.dumps({"v": 1, "seq": 0, "kind": "tool_call", "task_id": "t1", "tool": "a"}),
                huge_line,
                json.dumps({"v": 1, "seq": 1, "kind": "tool_call", "task_id": "t1", "tool": "b"}),
            ],
        )
        result = events_module.read_page(path, limit=50)
        assert result.skipped_oversized >= 1
        # The huge line should be skipped, so we get seq 0 and seq 1
        # But seq might not be contiguous due to the skip



# --- review fixes: recency, streaming position, window edges, page budget direction ------------


def _line(seq: int, kind: str, **fields) -> str:
    return json.dumps({"v": 1, "seq": seq, "observed_at": f"t{seq}", "kind": kind, **fields})


def test_read_recent_returns_the_newest_entries_not_the_oldest(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(i, "assistant_text", text=f"message {i}") for i in range(12)])

    assert events_module.read_recent(path, limit=3) == [
        "text  message 9",
        "text  message 10",
        "text  message 11",
    ]


def test_read_recent_places_a_streaming_block_where_its_newest_delta_is(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(
        path,
        [
            _line(1, "tool_call", call_id="c1", tool="Bash", category="shell", command="pytest -q"),
            _line(2, "assistant_delta", text="All ", message_id="m1", block_index=0),
            _line(3, "assistant_delta", text="green", message_id="m1", block_index=0),
            _line(4, "assistant_delta", text="stray", message_id="m1"),
        ],
    )

    assert events_module.read_recent(path) == ["shell  Bash  pytest -q", "text…  All green"]


def test_read_recent_drops_a_finished_block_and_a_delta_missing_half_its_identity(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(
        path,
        [
            _line(1, "assistant_delta", text="Done", message_id="m1", block_index=0),
            _line(2, "assistant_text", text="Done.", message_id="m1", block_index=0),
            _line(3, "assistant_delta", text="orphan", block_index=1),
        ],
    )

    assert events_module.read_recent(path) == ["text  Done."]


def test_read_recent_ignores_the_line_the_window_starts_inside_and_an_unfinished_last_line(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "t.events.jsonl"
    first = _line(1, "assistant_text", text="é" * 40)  # multi-byte, so the window cuts mid-line
    second = _line(2, "assistant_text", text="kept")
    path.write_bytes((first + "\n" + second + "\n").encode() + b'{"v": 1, "seq": 3, "kind": "assis')
    monkeypatch.setattr(events_module, "RECENT_ACTIVITY_MAX_BYTES", len(second) + 60)

    assert events_module.read_recent(path) == ["text  kept"]


def test_read_recent_names_a_failed_result_after_its_call_and_flattens_newlines(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(
        path,
        [
            _line(1, "tool_call", call_id="c1", tool="Bash", category="shell", command="make\nall"),
            _line(2, "tool_result", call_id="c1", ok=False, output_tail="\n\nerror: boom\nmore"),
            _line(3, "tool_result", call_id="gone", ok=False, output_tail=""),
        ],
    )

    assert events_module.read_recent(path) == [
        "shell  Bash  make all",
        "failed  Bash: error: boom",
        "failed  a tool call",
    ]


def test_read_page_budget_keeps_the_newest_events_reachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(i, "assistant_text", text="x" * 500) for i in range(10)])
    monkeypatch.setattr(events_module, "PAGE_BUDGET_BYTES", 1800)

    page = events_module.read_page(path, limit=10)

    assert [e["seq"] for e in page.events] == [7, 8, 9]
    assert page.has_more is True
    older = events_module.read_page(path, limit=10, before_seq=page.next_before_seq)
    assert [e["seq"] for e in older.events] == [4, 5, 6]


def test_read_page_budget_keeps_the_oldest_when_paging_forward(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(i, "assistant_text", text="x" * 500) for i in range(10)])
    monkeypatch.setattr(events_module, "PAGE_BUDGET_BYTES", 1800)

    page = events_module.read_page(path, limit=10, after_seq=2)

    assert [e["seq"] for e in page.events] == [3, 4, 5]
    assert page.has_more is True


def test_read_page_keeps_each_events_timestamp(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(1, "notice", text="hi")])

    assert events_module.read_page(path, limit=5).events == [
        {"seq": 1, "kind": "notice", "observed_at": "t1", "text": "hi"}
    ]


# --- review round 1: bounded forward reads, captured EOF, streaming suffix, per-event cap -------


def test_read_page_stops_at_the_size_the_log_had_when_opened(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(1, "notice", text="first")])
    real_parse = events_module._parse_event_line

    def append_while_reading(line: bytes):
        with path.open("a") as handle:
            handle.write(_line(2, "notice", text="appended mid-read") + "\n")
        return real_parse(line)

    monkeypatch.setattr(events_module, "_parse_event_line", append_while_reading)

    assert [e["seq"] for e in events_module.read_page(path, limit=10).events] == [1]


def test_read_page_leaves_an_unfinished_line_until_it_is_complete(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    whole = _line(2, "notice", text="two")
    path.write_bytes((_line(1, "notice", text="one") + "\n" + whole[:20]).encode())

    assert [e["seq"] for e in events_module.read_page(path, limit=10).events] == [1]
    with path.open("ab") as handle:
        handle.write((whole[20:] + "\n").encode())
    assert [e["seq"] for e in events_module.read_page(path, limit=10, after_seq=1).events] == [2]


def test_read_page_skips_an_oversized_line_without_buffering_it_whole(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(events_module, "MAX_LINE_BYTES", 200)
    monkeypatch.setattr(events_module, "CHUNK_SIZE", 64)
    path = tmp_path / "t.events.jsonl"
    _write_events_file(
        path,
        [_line(1, "notice", text="a"), _line(2, "notice", text="x" * 1000), _line(3, "notice", text="b")],
    )

    page = events_module.read_page(path, limit=10)

    assert [e["seq"] for e in page.events] == [1, 3]
    assert page.skipped_oversized == 1


def test_read_page_forward_holds_only_the_page_and_reports_more(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(i, "notice", text=str(i)) for i in range(100)])

    page = events_module.read_page(path, limit=2, after_seq=10)

    assert [e["seq"] for e in page.events] == [11, 12]
    assert page.has_more is True
    assert events_module.read_page(path, limit=5, after_seq=97).has_more is False


def test_read_page_replaces_an_event_too_wide_for_the_per_event_cap(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    wide = {f"k{i}": "v" * 1900 for i in range(200)}
    _write_events_file(path, [_line(1, "usage", usage=wide)])

    page = events_module.read_page(path, limit=5, kinds=["usage"])

    assert page.events[0]["oversized"] is True and page.events[0]["truncated"] is True
    assert len(json.dumps(page.events)) < events_module.EVENT_MAX_BYTES


def test_read_recent_shows_the_newest_words_of_a_long_streaming_block(tmp_path: Path) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(
        path, [_line(1, "assistant_delta", text="A" * 200 + "LATEST", message_id="m", block_index=0)]
    )

    [line] = events_module.read_recent(path)

    assert line.endswith("LATEST") and line.startswith("text…  …") and len(line) <= 160


def test_read_page_absorbs_an_error_raised_while_closing_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "t.events.jsonl"
    _write_events_file(path, [_line(1, "notice", text="a")])
    real_open = Path.open

    class FailingClose:
        def __init__(self, inner):
            self._inner = inner

        def __getattr__(self, name):
            return getattr(self._inner, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self._inner.close()
            raise OSError("close failed")

    monkeypatch.setattr(Path, "open", lambda self, *a, **k: FailingClose(real_open(self, *a, **k)))

    page = events_module.read_page(path, limit=5)

    assert page.has_more is False
