"""Takeover (A4.1): phase files, the session busy rule, refusals, the orchestration, and attach."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import backends, control, identity, lineage, store, takeover
from polybridge.tasks import SessionBusyError, TaskRegistry

CONTROLLER = {"pid": 111_111, "start_time": "Wed Jan  1 00:00:00 2026", "markers": []}
DEAD_OWNER = {"pid": 222_222, "start_time": "Wed Jan  1 00:00:00 2026", "markers": []}
SESSION = "0199a3f2-7c1e-7b8a-9d0e-123456789abc"
_real_identity_check = identity.identity_check


class Verdicts:
    """`identity.identity_check` stand-in: listed pids get a fixed verdict, every other pid the real
    `ps`-based answer — so a real child process can be checked for real beside synthetic owners."""

    def __init__(self) -> None:
        self.by_pid: dict[int, str] = {CONTROLLER["pid"]: "alive", DEAD_OWNER["pid"]: "dead"}

    def __call__(self, ident) -> str:
        if isinstance(ident, dict) and ident.get("pid") in self.by_pid:
            return self.by_pid[ident["pid"]]
        return _real_identity_check(ident)


@pytest.fixture
def verdicts(monkeypatch: pytest.MonkeyPatch) -> Verdicts:
    stub = Verdicts()
    monkeypatch.setattr(identity, "identity_check", stub)
    return stub


@pytest.fixture
def log_dir(tmp_path: Path) -> Path:
    path = tmp_path / "tasks"
    path.mkdir()
    return path


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    path = tmp_path / "repo"
    path.mkdir()
    return path


@pytest.fixture(autouse=True)
def controller_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(identity, "own_identity", lambda: CONTROLLER)
    monkeypatch.delenv("PB_TASK_ID", raising=False)


@pytest.fixture(autouse=True)
def claude_on_path(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        takeover.shutil, "which", lambda name: f"/opt/bin/{name}" if name == "claude" else None
    )


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def finished_record(repo: Path, **overrides) -> store.TaskRecord:
    now = datetime.now(timezone.utc)
    fields = dict(
        task_id="task-1",
        backend="claude",
        session_id=SESSION,
        repo_path=str(repo),
        started_at=_iso(now - timedelta(minutes=5)),
        markers=["claude", SESSION],
        pid=999_991,
        pgid=999_991,
        status="completed",
        exit_code=0,
        finished_at=_iso(now),
        owner=DEAD_OWNER,
    )
    fields |= overrides
    fields.setdefault("root_task_id", fields["task_id"])
    return store.TaskRecord(**fields)


def phase(log_dir: Path, task_id: str, n: int, name: str, payload: dict) -> None:
    (log_dir / f"{task_id}.takeover.{n}.{name}").write_text(json.dumps(payload))


def req_payload(at: datetime, controller: dict = CONTROLLER, session_id: str = SESSION) -> dict:
    return {"at": _iso(at), "session_id": session_id, "by": controller, "lease_seconds": 60}


def phases_on_disk(log_dir: Path, task_id: str = "task-1") -> list[str]:
    return sorted(p.name[len(task_id) + 1 :] for p in log_dir.iterdir() if ".takeover." in p.name)


def failed_reason(log_dir: Path, n: int, task_id: str = "task-1") -> str:
    return json.loads((log_dir / f"{task_id}.takeover.{n}.failed").read_text())["reason"]


# --- the busy rule ---------------------------------------------------------------------------


NOW = datetime(2026, 9, 25, 12, 0, 0, tzinfo=timezone.utc)


def attempt_of(log_dir: Path) -> control.TakeoverAttempt | None:
    return control.takeover_attempt(log_dir, "task-1")


def test_no_attempt_is_not_busy(log_dir: Path) -> None:
    assert control.takeover_busy(attempt_of(log_dir), NOW) is False


@pytest.mark.parametrize("age", [0, 60, 119])
def test_a_pending_attempt_is_busy_through_the_window(log_dir: Path, verdicts: Verdicts, age: int) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(seconds=age)))
    verdicts.by_pid[CONTROLLER["pid"]] = "dead"

    assert control.takeover_busy(attempt_of(log_dir), NOW) is True


def test_a_pending_attempt_stays_busy_past_the_window_while_its_controller_lives(
    log_dir: Path, verdicts: Verdicts
) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(seconds=600)))

    assert control.takeover_busy(attempt_of(log_dir), NOW) is True
    verdicts.by_pid[CONTROLLER["pid"]] = "undecidable"
    assert control.takeover_busy(attempt_of(log_dir), NOW) is True
    verdicts.by_pid[CONTROLLER["pid"]] = "dead"
    assert control.takeover_busy(attempt_of(log_dir), NOW) is False


def test_a_ready_attempt_is_busy_for_the_window_after_the_later_of_req_and_ready(
    log_dir: Path, verdicts: Verdicts
) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(seconds=200)))
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(NOW - timedelta(seconds=100))})

    assert control.takeover_busy(attempt_of(log_dir), NOW) is True
    assert control.takeover_busy(attempt_of(log_dir), NOW + timedelta(seconds=19)) is True
    assert control.takeover_busy(attempt_of(log_dir), NOW + timedelta(seconds=21)) is False


@pytest.mark.parametrize(("verdict", "busy"), [("alive", True), ("undecidable", True), ("dead", False)])
def test_an_attached_attempt_is_busy_until_the_terminal_is_confirmed_gone(
    log_dir: Path, verdicts: Verdicts, verdict: str, busy: bool
) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(hours=2)))
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(NOW - timedelta(hours=2))})
    phase(log_dir, "task-1", 1, "attach", {"at": _iso(NOW), "pid": 333, "start_time": "x", "markers": []})
    verdicts.by_pid[333] = verdict

    assert control.takeover_busy(attempt_of(log_dir), NOW) is busy


def test_a_failed_attempt_is_never_busy(log_dir: Path) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW))
    phase(log_dir, "task-1", 1, "failed", {"at": _iso(NOW), "reason": "x"})

    assert control.takeover_busy(attempt_of(log_dir), NOW) is False


def test_unparsable_phase_files_count_as_busy(log_dir: Path) -> None:
    (log_dir / "task-1.takeover.1.req").write_text("not json")
    assert control.takeover_busy(attempt_of(log_dir), NOW) is True

    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(hours=1)))
    (log_dir / "task-1.takeover.1.ready").write_text("{}")  # no `at`
    assert control.takeover_busy(attempt_of(log_dir), NOW) is True


def test_only_the_latest_attempt_decides(log_dir: Path, verdicts: Verdicts) -> None:
    phase(log_dir, "task-1", 9, "req", req_payload(NOW))
    phase(log_dir, "task-1", 9, "failed", {"at": _iso(NOW), "reason": "x"})
    phase(log_dir, "task-1", 10, "req", req_payload(NOW))

    attempt = attempt_of(log_dir)
    assert attempt is not None and attempt.n == 10
    assert control.takeover_busy(attempt, NOW) is True


# --- reservations, live_session_ids, and resume ---------------------------------------------


def test_live_session_ids_includes_a_session_held_by_a_takeover(log_dir: Path, repo: Path) -> None:
    store.write(log_dir, finished_record(repo))
    now = datetime.now(timezone.utc)
    phase(log_dir, "task-1", 1, "req", req_payload(now))

    assert control.takeover_reservations(log_dir) == {"task-1": SESSION}
    assert SESSION in store.live_session_ids(log_dir)


def test_a_reservation_falls_back_to_the_records_session_when_req_is_unparsable(
    log_dir: Path, repo: Path
) -> None:
    store.write(log_dir, finished_record(repo))
    (log_dir / "task-1.takeover.1.req").write_text("garbage")

    assert control.takeover_reservations(log_dir) == {"task-1": SESSION}


async def test_resume_is_refused_from_req_through_the_window_and_while_attached(
    log_dir: Path, repo: Path, verdicts: Verdicts, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Cowork `resume_task` on a taken-over session gets the existing `SessionBusyError`."""
    record = finished_record(repo)
    store.write(log_dir, record)
    registry = TaskRegistry(log_dir=log_dir, owner=CONTROLLER)
    spawned: list[str] = []

    async def fake_spawn(*args, **kwargs):
        spawned.append("spawned")
        raise RuntimeError("stop here")

    monkeypatch.setattr(registry, "_spawn", fake_spawn)
    now = datetime.now(timezone.utc)

    phase(log_dir, "task-1", 1, "req", req_payload(now))
    with pytest.raises(SessionBusyError, match="takeover"):
        await registry.resume_record(record, "go on")

    phase(log_dir, "task-1", 1, "ready", {"at": _iso(now)})
    with pytest.raises(SessionBusyError):
        await registry.resume_record(record, "go on")

    phase(log_dir, "task-1", 1, "attach", {"at": _iso(now), "pid": 444, "start_time": "x", "markers": []})
    verdicts.by_pid[444] = "alive"
    with pytest.raises(SessionBusyError):
        await registry.resume_record(record, "go on")

    verdicts.by_pid[444] = "dead"
    with pytest.raises(RuntimeError, match="stop here"):
        await registry.resume_record(record, "go on")
    assert spawned == ["spawned"]


# --- begin_takeover ------------------------------------------------------------------------


def test_begin_takeover_numbers_attempts_and_only_retries_after_failed(
    log_dir: Path, verdicts: Verdicts
) -> None:
    assert control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION) == 1
    req = json.loads((log_dir / "task-1.takeover.1.req").read_text())
    assert req["session_id"] == SESSION and req["by"] == CONTROLLER and req["lease_seconds"] == 60

    with pytest.raises(control.TakeoverRefused) as refused:
        control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION)
    assert refused.value.code == "takeover_in_progress"

    control.mark_takeover_failed(log_dir, "task-1", 1, "x")
    assert control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION) == 2


def test_begin_takeover_recovers_an_abandoned_pending_attempt(log_dir: Path, verdicts: Verdicts) -> None:
    dead = {"pid": 555, "start_time": "x", "markers": []}
    verdicts.by_pid[555] = "dead"
    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(seconds=61), controller=dead))

    assert control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION, now=NOW) == 2
    assert failed_reason(log_dir, 1) == "controller died"


def test_begin_takeover_closes_a_ready_attempt_whose_window_expired(log_dir: Path) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW - timedelta(seconds=300)))
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(NOW - timedelta(seconds=290))})

    assert control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION, now=NOW) == 2
    assert failed_reason(log_dir, 1) == "attach window expired"


def test_begin_takeover_refuses_while_ready_or_attached(log_dir: Path, verdicts: Verdicts) -> None:
    phase(log_dir, "task-1", 1, "req", req_payload(NOW))
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(NOW)})
    with pytest.raises(control.TakeoverRefused) as refused:
        control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION, now=NOW)
    assert refused.value.code == "takeover_pending_attach"

    phase(log_dir, "task-1", 1, "attach", {"at": _iso(NOW), "pid": 666, "start_time": "x", "markers": []})
    verdicts.by_pid[666] = "dead"
    with pytest.raises(control.TakeoverRefused) as refused:
        control.begin_takeover(log_dir, "task-1", controller=CONTROLLER, session_id=SESSION, now=NOW)
    assert refused.value.code == "already_taken_over"


# --- taken_over in listings -----------------------------------------------------------------


def test_taken_over_appears_only_with_ready_and_no_failed(log_dir: Path, repo: Path) -> None:
    store.write(log_dir, finished_record(repo))
    now = datetime.now(timezone.utc)

    def listing() -> dict:
        return store.brief(log_dir, store.read(log_dir, "task-1"))

    assert "taken_over" not in listing()
    phase(log_dir, "task-1", 1, "req", req_payload(now))
    assert "taken_over" not in listing()
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(now)})
    assert listing()["taken_over"] is True
    assert listing()["taken_over_note"] == "taken over by the user in the Monitor"
    snap = store.snapshot(log_dir, store.read(log_dir, "task-1"))
    assert snap["taken_over"] is True and snap["status"] == "completed"
    phase(log_dir, "task-1", 1, "failed", {"at": _iso(now), "reason": "x"})
    assert "taken_over" not in listing()


def test_a_live_tasks_brief_carries_taken_over_too(log_dir: Path, repo: Path) -> None:
    from polybridge.tasks import Task

    task = Task(
        task_id="task-1",
        backend="claude",
        session_id=SESSION,
        repo_path=repo,
        prompt="x",
        max_turns=None,
        log_path=log_dir / "task-1.jsonl",
        started_at=datetime.now(timezone.utc),
    )
    assert "taken_over" not in task.brief()
    phase(log_dir, "task-1", 1, "req", req_payload(NOW))
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(NOW)})
    assert task.brief()["taken_over"] is True
    assert task.snapshot()["taken_over"] is True


# --- take_over: refusals ----------------------------------------------------------------------


def _never_cancel():
    raise AssertionError("a finished task must not be cancelled")


async def _take_over(log_dir: Path, task_id: str = "task-1", **kwargs):
    kwargs.setdefault("registry_factory", _never_cancel)
    return await takeover.take_over(log_dir, task_id, **kwargs)


async def test_an_agent_caller_is_refused_before_anything_is_written(
    log_dir: Path, repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.write(log_dir, finished_record(repo))

    with pytest.raises(control.TakeoverRefused) as refused:
        await _take_over(log_dir, environ={"PB_TASK_ID": "no-such-task"})
    assert refused.value.code == "agent_caller"

    caller = lineage.Caller(finished_record(repo, task_id="parent"), "ancestry")
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: caller)
    with pytest.raises(control.TakeoverRefused) as refused:
        await _take_over(log_dir)
    assert refused.value.code == "agent_caller"
    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", os.getpid())
    assert refused.value.code == "agent_caller"

    assert phases_on_disk(log_dir) == []


async def test_unknown_and_invalid_tasks_are_refused(log_dir: Path) -> None:
    for task_id, code in (("../x", "invalid_task_id"), ("nope", "unknown_task")):
        with pytest.raises(control.TakeoverRefused) as refused:
            await _take_over(log_dir, task_id)
        assert refused.value.code == code


@pytest.mark.parametrize(
    ("setup", "code"),
    [
        ("no_session", "no_session"),
        ("no_command", "no_interactive_command"),
        ("no_binary", "binary_not_found"),
        ("repo_gone", "repo_unavailable"),
        ("other_holder", "session_busy"),
        ("undecidable", "not_stopped"),
    ],
)
async def test_each_refusal_writes_failed_and_a_retry_opens_the_next_attempt(
    log_dir: Path,
    repo: Path,
    verdicts: Verdicts,
    monkeypatch: pytest.MonkeyPatch,
    setup: str,
    code: str,
) -> None:
    record = finished_record(repo)
    if setup == "no_session":
        record = finished_record(repo, session_id=None)
    elif setup == "no_command":
        monkeypatch.setattr(backends.ClaudeBackend, "interactive_resume_argv", lambda self, s, r: None)
    elif setup == "no_binary":
        monkeypatch.setattr(takeover.shutil, "which", lambda name: None)
    elif setup == "repo_gone":
        record = finished_record(repo, repo_path=str(repo / "gone"))
    elif setup == "other_holder":
        store.write(
            log_dir,
            finished_record(
                repo, task_id="other", status="running", exit_code=None, pid=777, start_time="x"
            ),
        )
        verdicts.by_pid[777] = "alive"
    elif setup == "undecidable":
        # A legacy record (no start_time) still marked running: the markers-seen fallback is not
        # accepted for a takeover, so its liveness is undecidable.
        record = finished_record(repo, status="running", exit_code=None, pid=os.getpid(), start_time=None, markers=[])
    store.write(log_dir, record)

    with pytest.raises(control.TakeoverRefused) as refused:
        await _take_over(log_dir)
    assert refused.value.code == code
    assert "task-1.takeover.1.failed" in {p.name for p in log_dir.iterdir()}
    assert failed_reason(log_dir, 1) == str(refused.value)
    assert not control.taken_over(log_dir, "task-1")

    with pytest.raises(control.TakeoverRefused):
        await _take_over(log_dir)
    assert (log_dir / "task-1.takeover.2.req").exists()
    assert (log_dir / "task-1.takeover.2.failed").exists()


async def test_a_finished_task_keeps_its_status_and_gains_taken_over(
    log_dir: Path, repo: Path, verdicts: Verdicts
) -> None:
    store.write(log_dir, finished_record(repo, status="failed", exit_code=1))

    result = await _take_over(log_dir)

    assert set(result) == {"argv", "cwd", "session_id", "note"}
    assert result["argv"] == ["/opt/bin/claude", "--resume", SESSION]
    assert result["cwd"] == str(repo)
    assert result["session_id"] == SESSION
    assert "does not apply" in result["note"] and "freedom: write_in_repo" in result["note"]
    assert "already finished (failed)" in result["note"]
    brief = store.brief(log_dir, store.read(log_dir, "task-1"))
    assert brief["status"] == "failed"
    assert brief["taken_over"] is True
    assert SESSION in store.live_session_ids(log_dir)


# --- take_over: a live task -------------------------------------------------------------------


@pytest.fixture
def sleeper():
    procs: list[subprocess.Popen] = []

    def start(seconds: int = 60) -> subprocess.Popen:
        proc = subprocess.Popen(["sleep", str(seconds)], start_new_session=True)
        procs.append(proc)
        return proc

    yield start
    for proc in procs:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(timeout=5)


@pytest.fixture
def orphan_sleeper():
    """A `sleep` reparented to launchd/init — reaped by it the moment it dies, as a real agent is by
    its owning server. A child of this test process would linger as a zombie that `ps` still
    reports with its original start time, which is not what a real headless run looks like."""
    pids: list[int] = []

    def start() -> tuple[int, int]:
        shell = subprocess.run(
            ["sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $$ $!"],
            capture_output=True,
            text=True,
            check=True,
            start_new_session=True,
        )
        pgid, pid = (int(x) for x in shell.stdout.split())
        pids.append(pid)
        return pid, pgid

    yield start
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _captured(pid: int, markers: list[str]) -> dict:
    for _ in range(50):
        got = identity.capture(pid, markers)
        if got is not None:
            return got
        time.sleep(0.05)
    raise AssertionError("could not capture the test process")


async def test_a_live_task_is_stopped_confirmed_dead_and_its_session_held(
    log_dir: Path, repo: Path, verdicts: Verdicts, orphan_sleeper
) -> None:
    pid, pgid = orphan_sleeper()
    ident = _captured(pid, ["sleep"])
    store.write(
        log_dir,
        finished_record(
            repo,
            status="running",
            exit_code=None,
            finished_at=None,
            pid=pid,
            pgid=pgid,
            start_time=ident["start_time"],
            markers=["sleep"],
        ),
    )

    result = await takeover.take_over(
        log_dir, "task-1", registry_factory=lambda: TaskRegistry(log_dir=log_dir, owner=CONTROLLER)
    )

    assert _real_identity_check(ident) == "dead"
    assert "headless run is stopped" in result["note"]
    record = store.read(log_dir, "task-1")
    assert store.brief(log_dir, record)["status"] == "cancelled"
    assert store.brief(log_dir, record)["taken_over"] is True
    assert (log_dir / "task-1.takeover.1.ready").exists()
    assert SESSION in store.live_session_ids(log_dir)


async def test_a_survivor_refuses_the_takeover(
    log_dir: Path, repo: Path, verdicts: Verdicts, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.write(log_dir, finished_record(repo, status="running", exit_code=None, start_time="x"))
    verdicts.by_pid[999_991] = "alive"
    monkeypatch.setattr(
        identity, "check_detail", lambda ident: ("alive", "start_time_match")
    )

    class _Registry:
        async def cancel_cascade(self, task_id):
            return {"sigkill_survivors": [task_id], "not_signalled": [], "owner_still_settling": []}

    with pytest.raises(control.TakeoverRefused) as refused:
        await takeover.take_over(log_dir, "task-1", registry_factory=_Registry)
    assert refused.value.code == "not_stopped"
    assert failed_reason(log_dir, 1).startswith("the headless run could not be confirmed stopped")
    assert control.takeover_reservations(log_dir) == {}


async def test_a_run_that_starts_on_the_session_before_ready_refuses(
    log_dir: Path, repo: Path, verdicts: Verdicts, monkeypatch: pytest.MonkeyPatch
) -> None:
    store.write(log_dir, finished_record(repo, status="running", exit_code=None, start_time="x"))
    monkeypatch.setattr(identity, "check_detail", lambda ident: ("alive", "start_time_match"))
    verdicts.by_pid[999_991] = "dead"

    class _Registry:
        async def cancel_cascade(self, task_id):
            store.write(
                log_dir,
                finished_record(repo, task_id="late", status="running", exit_code=None, pid=888, start_time="x"),
            )
            verdicts.by_pid[888] = "alive"
            return {"sigkill_survivors": [], "not_signalled": [], "owner_still_settling": []}

    with pytest.raises(control.TakeoverRefused) as refused:
        await takeover.take_over(log_dir, "task-1", registry_factory=_Registry)
    assert refused.value.code == "session_busy"
    assert (log_dir / "task-1.takeover.1.failed").exists()
    assert not (log_dir / "task-1.takeover.1.ready").exists()


# --- attach ---------------------------------------------------------------------------------


def ready_attempt(log_dir: Path, repo: Path, *, age: float = 1.0) -> None:
    store.write(log_dir, finished_record(repo))
    at = datetime.now(timezone.utc) - timedelta(seconds=age)
    phase(log_dir, "task-1", 1, "req", req_payload(at))
    phase(log_dir, "task-1", 1, "ready", {"at": _iso(at)})


def test_attach_records_the_terminal_and_the_session_stays_busy_while_it_lives(
    log_dir: Path, repo: Path, sleeper
) -> None:
    ready_attempt(log_dir, repo)
    proc = sleeper()
    _captured(proc.pid, [])

    result = takeover.attach(log_dir, "task-1", proc.pid)

    assert result["status"] == "attached" and result["pid"] == proc.pid and result["attempt"] == 1
    payload = json.loads((log_dir / "task-1.takeover.1.attach").read_text())
    assert payload["markers"] == ["claude", SESSION] and payload["start_time"]
    # `sleep` does not show the markers, so it reads undecidable — which is busy, as is alive.
    assert SESSION in store.live_session_ids(log_dir)

    os.killpg(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)
    assert SESSION not in store.live_session_ids(log_dir)
    assert control.taken_over(log_dir, "task-1")


def test_attach_refusals(log_dir: Path, repo: Path, verdicts: Verdicts, sleeper) -> None:
    store.write(log_dir, finished_record(repo))
    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", os.getpid())
    assert refused.value.code == "not_ready"
    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", 1)
    assert refused.value.code == "invalid_pid"

    ready_attempt(log_dir, repo)
    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", 2_000_000_000)
    assert refused.value.code == "pid_not_found"
    assert not (log_dir / "task-1.takeover.1.failed").exists()

    proc = sleeper()
    _captured(proc.pid, [])
    takeover.attach(log_dir, "task-1", proc.pid)
    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", proc.pid)
    assert refused.value.code == "already_attached"


def test_attach_after_the_window_writes_failed(log_dir: Path, repo: Path) -> None:
    ready_attempt(log_dir, repo, age=121)

    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", os.getpid())

    assert refused.value.code == "window_expired"
    assert failed_reason(log_dir, 1) == "attach window expired"
    assert not control.taken_over(log_dir, "task-1")


def test_attach_with_another_run_on_the_session_writes_failed(
    log_dir: Path, repo: Path, verdicts: Verdicts
) -> None:
    ready_attempt(log_dir, repo)
    store.write(
        log_dir,
        finished_record(repo, task_id="other", status="running", exit_code=None, pid=777, start_time="x"),
    )
    verdicts.by_pid[777] = "alive"

    with pytest.raises(control.TakeoverRefused) as refused:
        takeover.attach(log_dir, "task-1", os.getpid())

    assert refused.value.code == "session_busy"
    assert failed_reason(log_dir, 1) == "another run holds the session"


def test_a_descendant_on_the_same_session_does_not_count_as_another_holder(
    log_dir: Path, repo: Path, verdicts: Verdicts
) -> None:
    ready_attempt(log_dir, repo)
    store.write(
        log_dir,
        finished_record(
            repo, task_id="child", spawned_by="task-1", status="running", exit_code=None, pid=778, start_time="x"
        ),
    )
    verdicts.by_pid[778] = "alive"

    assert takeover.other_session_holders(log_dir, "task-1", SESSION) == []
