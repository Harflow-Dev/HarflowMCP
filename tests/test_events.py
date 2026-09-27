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
    assert task.start_dirty is False


async def test_git_baseline_reports_dirty_when_the_repo_has_a_pending_change(
    git_repo: Path, registered
) -> None:
    subprocess.run(["git", "-C", str(git_repo), "commit", "-qm", "x", "--allow-empty"], check=True)
    (git_repo / "untracked.txt").write_text("hi")
    backend = registered(_RecordingBackend(_two_line_script()))
    registry = TaskRegistry(log_dir=git_repo / ".streams")

    task = await registry.start("hi", git_repo, backend=backend)
    await task.done.wait()

    assert task.start_dirty is True


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
_README = Path(__file__).resolve().parent.parent / "README.md"


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


def test_the_readme_lists_exactly_event_kinds() -> None:
    text = " ".join(_README.read_text(encoding="utf-8").split())
    match = _re.search(r"`kind` — one of (.*?)\. ", text)
    assert match is not None, "README's events.jsonl kind list not found"
    documented = set(_re.findall(r"`(\w+)`", match.group(1)))
    assert documented == events_module.EVENT_KINDS
