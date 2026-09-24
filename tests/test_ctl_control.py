"""`polybridge-ctl` control commands (A4.2): cancel, takeover, takeover-attach, and the run/resume
fork-and-handshake lifecycle.

The handshake tests really fork, from a fresh single-threaded interpreter (`ctl_driver.py`) rather
than from pytest's own process, which carries worker threads by then — forking that can deadlock the
child. The driver registers a fake backend and runs the real `ctl.main`."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from polybridge import backends, ctl, identity, store
from polybridge.tasks import default_log_dir

SESSION = "0199a3f2-7c1e-7b8a-9d0e-123456789abc"
CODEX_READ_ONLY = backends.get("codex").enforcement("read_only").as_dict()
DRIVER = Path(__file__).with_name("ctl_driver.py")


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("PB_TASK_ID", raising=False)
    return tmp_path


def drive(
    *args: str, script: str | None = None, caller: dict | None = None, **extra_env: str
) -> tuple[int, str, str]:
    env = dict(os.environ) | extra_env
    env.pop("PB_TASK_ID", None)
    if script is not None:
        env["PB_FAKE_SCRIPT"] = script
    if caller is not None:
        env["PB_FAKE_CALLER"] = json.dumps(caller)
    result = subprocess.run(
        [sys.executable, str(DRIVER), *args],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    return result.returncode, result.stdout, result.stderr


def _one(stdout: str) -> dict:
    lines = [line for line in stdout.splitlines() if line.strip()]
    assert len(lines) == 1, lines
    return json.loads(lines[0])


def _doc(capsys: pytest.CaptureFixture) -> dict:
    return _one(capsys.readouterr().out)


def _settled(task_id: str, timeout: float = 15.0) -> store.TaskRecord:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = store.read(default_log_dir(), task_id)
        if record is not None and record.status in store.TERMINAL_RECORD_STATUSES:
            return record
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not settle")


# --- run / resume: the handshake ------------------------------------------------------------


def test_run_returns_the_task_id_and_the_child_owns_the_task_to_the_end(git_repo: Path) -> None:
    code, out, err = drive(
        "ctl", "run", "--backend", "fake", "--repo", str(git_repo), "--prompt", "hi", "--json"
    )

    assert code == 0, err
    doc = _one(out)
    assert doc["v"] == 1 and set(doc["result"]) == {"task_id"}
    record = _settled(doc["result"]["task_id"])
    assert record.status == "completed" and record.exit_code == 0
    # Owned by the forked child, which exits once the task settles — not by the process that
    # printed, which had already exited.
    assert record.owner is not None
    deadline = time.monotonic() + 10
    while identity.identity_check(record.owner) != "dead":
        assert time.monotonic() < deadline, "the owning child never exited"
        time.sleep(0.1)
    assert (default_log_dir().parent / "ctl.log").exists()


@pytest.mark.parametrize(
    "argv_tail",
    [
        ["--backend", "nope", "--prompt", "hi"],
        ["--backend", "fake", "--prompt", "   "],
        ["--backend", "fake", "--prompt", "hi", "--freedom", "godmode"],
    ],
)
def test_run_reports_validation_errors_from_the_child(git_repo: Path, argv_tail) -> None:
    code, out, _ = drive("ctl", "run", "--repo", str(git_repo), *argv_tail, "--json")

    assert code == 1
    doc = _one(out)
    assert doc["v"] == 1 and doc["error"]["code"] == "invalid_params" and doc["error"]["message"]
    assert store.read_all(default_log_dir()) == []


def test_run_reports_a_bad_repo(tmp_path: Path) -> None:
    code, out, _ = drive(
        "ctl", "run", "--backend", "fake", "--repo", str(tmp_path / "missing"), "--prompt", "hi", "--json"
    )

    assert code == 1
    doc = _one(out)
    assert doc["error"]["code"] == "invalid_params"
    assert "does not exist" in doc["error"]["message"]


def test_run_applies_the_nested_dispatch_caps(git_repo: Path) -> None:
    parent = {
        "task_id": "parent",
        "backend": "codex",
        "session_id": "p",
        "repo_path": str(git_repo),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "freedom": "read_only",
        "enforcement": CODEX_READ_ONLY,
        "root_task_id": "parent",
        "max_depth": 2,
    }

    code, out, _ = drive(
        "ctl", "run", "--backend", "fake", "--repo", str(git_repo), "--prompt", "hi", "--json",
        caller=parent,
    )

    assert code == 1
    assert _one(out)["error"]["code"] == "nested_dispatch_refused"
    assert store.read_all(default_log_dir()) == []


def test_resume_hands_back_a_new_task_on_the_same_session(git_repo: Path) -> None:
    now = datetime.now(timezone.utc).isoformat()
    store.write(
        default_log_dir(),
        store.TaskRecord(
            task_id="first",
            backend="fake",
            session_id=SESSION,
            repo_path=str(git_repo),
            started_at=now,
            status="completed",
            exit_code=0,
            finished_at=now,
        ),
    )

    code, out, err = drive("ctl", "resume", "first", "go on", "--json")

    assert code == 0, err
    record = _settled(_one(out)["result"]["task_id"])
    assert record.session_id == SESSION and record.parent_task_id == "first"


def test_resume_errors_come_back_as_json() -> None:
    code, out, _ = drive("ctl", "resume", "nope", "go on", "--json")

    assert code == 1
    assert _one(out)["error"] == {"code": "invalid_params", "message": "unknown task_id: nope"}


def test_a_silent_child_is_reported_unknown_terminated_and_reaped() -> None:
    started = time.monotonic()
    code, out, err = drive("hang", "1")

    assert code == 0, err
    outcome = _one(out)
    assert outcome["kind"] == "unknown"
    assert "polybridge-ctl list" in outcome["payload"]["message"]
    assert time.monotonic() - started < 20
    with pytest.raises(ProcessLookupError):
        os.kill(outcome["child_pid"], 0)


def test_a_timed_out_child_cancels_the_task_it_already_started(git_repo: Path) -> None:
    code, out, err = drive("start_then_hang", "2", str(git_repo), script="sleep 30")

    assert code == 0, err
    assert _one(out)["kind"] == "unknown"
    (record,) = store.read_all(default_log_dir())
    assert record.status == "cancelled"
    assert identity.identity_check(
        identity.task_identity(record.pid, record.start_time, record.markers)
    ) == "dead"


def test_a_child_whose_cancel_fails_keeps_owning_its_task_until_it_settles(git_repo: Path) -> None:
    """Codex round 1: the child used to exit after a failed cancel, leaving the agent running with
    nobody draining its pipes. Now it keeps owning the task — the parent, whose reap grace runs out
    first, does not SIGKILL it and says so — and the task settles observed, with its exit code."""
    started = time.monotonic()
    code, out, err = drive(
        "start_then_hang", "1", str(git_repo), script="sleep 4",
        PB_FAKE_CANCEL_FAILS="1", PB_FAKE_REAP_GRACE="1",
    )

    assert code == 0, err
    outcome = _one(out)
    assert outcome["kind"] == "unknown"
    assert "had not exited" in outcome["payload"]["message"]
    assert "was stopped" not in outcome["payload"]["message"]
    assert time.monotonic() - started < 4
    (record,) = store.read_all(default_log_dir())
    settled = _settled(record.task_id, timeout=20)
    assert settled.status == "completed" and settled.exit_code == 0
    deadline = time.monotonic() + 10
    while True:
        try:
            os.kill(outcome["child_pid"], 0)
        except ProcessLookupError:
            break
        assert time.monotonic() < deadline, "the owning child never exited"
        time.sleep(0.1)


def test_the_unknown_document_is_versioned_and_exits_3(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    from polybridge import detached

    monkeypatch.setattr(
        detached,
        "run_detached",
        lambda *a, **k: detached.Outcome("unknown", {"message": "no word"}, 0),
    )

    assert ctl.main(["run", "--backend", "x", "--repo", "/", "--prompt", "p", "--json"]) == 3
    assert _doc(capsys) == {"v": 1, "unknown": {"message": "no word"}}


def _dead_owner() -> dict:
    """An identity `ps` confirms dead: a process that has exited and been reaped (a reused pid
    would show a different start time). A made-up huge pid makes `ps` error, which reads as
    undecidable and sends a cascade down its slow, owner-still-alive path."""
    proc = subprocess.Popen(["true"])
    proc.wait()
    return {"pid": proc.pid, "start_time": "Thu Jan  1 00:00:00 1970", "markers": []}


# --- cancel ----------------------------------------------------------------------------------


def test_cancel_stops_a_task_whose_owner_is_gone(
    capsys, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    shell = subprocess.run(
        ["sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $$ $!"],
        capture_output=True, text=True, check=True, start_new_session=True,
    )
    pgid, pid = (int(x) for x in shell.stdout.split())
    try:
        ident = None
        for _ in range(50):
            ident = identity.capture(pid, ["sleep"])
            if ident:
                break
            time.sleep(0.05)
        dead_owner = _dead_owner()
        store.write(
            default_log_dir(),
            store.TaskRecord(
                task_id="t1",
                backend="claude",
                session_id="s",
                repo_path=str(tmp_path),
                started_at=datetime.now(timezone.utc).isoformat(),
                markers=["sleep"],
                pid=pid,
                pgid=pgid,
                start_time=ident["start_time"],
                owner=dead_owner,
            ),
        )

        assert ctl.main(["cancel", "t1", "--json"]) == 0
        doc = _doc(capsys)
        assert doc["v"] == 1
        assert doc["result"]["status"] == "cancelled"
        assert set(doc["result"]["cascade"]) >= {"cancelled_descendants", "sigkill_survivors"}
    finally:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_cancel_refusals(capsys) -> None:
    assert ctl.main(["cancel", "../x", "--json"]) == 1
    assert _doc(capsys)["error"]["code"] == "invalid_task_id"
    assert ctl.main(["cancel", "nope", "--json"]) == 1
    assert _doc(capsys)["error"]["code"] == "unknown_task"


# --- takeover / takeover-attach ------------------------------------------------------------


def _finished_claude(repo: Path) -> None:
    now = datetime.now(timezone.utc).isoformat()
    store.write(
        default_log_dir(),
        store.TaskRecord(
            task_id="t1",
            backend="claude",
            session_id=SESSION,
            repo_path=str(repo),
            started_at=now,
            status="completed",
            exit_code=0,
            finished_at=now,
            root_task_id="t1",
        ),
    )


def test_takeover_json_for_a_finished_task(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    from polybridge import takeover

    monkeypatch.setattr(takeover.shutil, "which", lambda name: f"/opt/bin/{name}")
    _finished_claude(tmp_path)

    assert ctl.main(["takeover", "t1", "--json"]) == 0
    doc = _doc(capsys)
    assert doc["v"] == 1
    assert doc["result"]["argv"] == ["/opt/bin/claude", "--resume", SESSION]
    assert doc["result"]["cwd"] == str(tmp_path)

    assert ctl.main(["status", "t1", "--json"]) == 0
    status = _doc(capsys)["task"]
    assert status["status"] == "completed" and status["taken_over"] is True


def test_takeover_refuses_an_agent_caller(tmp_path: Path, capsys, monkeypatch) -> None:
    _finished_claude(tmp_path)
    monkeypatch.setenv("PB_TASK_ID", "t0")

    assert ctl.main(["takeover", "t1", "--json"]) == 1
    assert _doc(capsys)["error"]["code"] == "agent_caller"
    assert ctl.main(["takeover-attach", "t1", "--pid", str(os.getpid()), "--json"]) == 1
    assert _doc(capsys)["error"]["code"] == "agent_caller"
    assert not any(".takeover." in p.name for p in default_log_dir().iterdir())


def test_takeover_attach_json(tmp_path: Path, capsys, monkeypatch) -> None:
    from polybridge import takeover

    monkeypatch.setattr(takeover.shutil, "which", lambda name: f"/opt/bin/{name}")
    _finished_claude(tmp_path)
    assert ctl.main(["takeover-attach", "t1", "--pid", str(os.getpid()), "--json"]) == 1
    assert _doc(capsys)["error"]["code"] == "not_ready"

    assert ctl.main(["takeover", "t1", "--json"]) == 0
    capsys.readouterr()
    assert ctl.main(["takeover-attach", "t1", "--pid", str(os.getpid()), "--json"]) == 0
    result = _doc(capsys)["result"]
    assert result["status"] == "attached" and result["pid"] == os.getpid()
    assert SESSION in store.live_session_ids(default_log_dir())


def _orphan_sleep() -> tuple[int, int, dict]:
    shell = subprocess.run(
        ["sh", "-c", "sleep 60 >/dev/null 2>&1 & echo $$ $!"],
        capture_output=True, text=True, check=True, start_new_session=True,
    )
    pgid, pid = (int(x) for x in shell.stdout.split())
    for _ in range(50):
        ident = identity.capture(pid, ["sleep"])
        if ident:
            return pid, pgid, ident
        time.sleep(0.05)
    raise AssertionError("could not capture the sleeper")


def _running_record(tmp_path: Path, pid: int, pgid: int, ident: dict) -> None:
    store.write(
        default_log_dir(),
        store.TaskRecord(
            task_id="t1",
            backend="claude",
            session_id="s",
            repo_path=str(tmp_path),
            started_at=datetime.now(timezone.utc).isoformat(),
            markers=["sleep"],
            pid=pid,
            pgid=pgid,
            start_time=ident["start_time"],
            owner=_dead_owner(),
        ),
    )


def _flaky_sig(monkeypatch: pytest.MonkeyPatch, failures: int) -> None:
    from polybridge import control

    real = control.write_phase
    left = {"n": failures}

    def flaky(log_dir, task_id, family, n, phase, payload):
        if phase == "sig" and left["n"] > 0:
            left["n"] -= 1
            raise control.PhaseWriteError("disk hiccup")
        return real(log_dir, task_id, family, n, phase, payload)

    monkeypatch.setattr(control, "write_phase", flaky)


def test_cancel_stays_alive_until_a_failing_sig_write_lands(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Codex round 2: a delivered SIGTERM whose `.sig` write failed is retried in the background;
    a ctl process exiting first would cancel that retry, and lease recovery would later fail the
    attempt, so a deliberately cancelled run would not read as cancelled."""
    pid, pgid, ident = _orphan_sleep()
    try:
        _running_record(tmp_path, pid, pgid, ident)
        _flaky_sig(monkeypatch, failures=4)  # 3 inline attempts + the first background retry

        assert ctl.main(["cancel", "t1", "--json"]) == 0

        result = _doc(capsys)["result"]
        assert "unrecorded_phase_writes" not in result
        assert (default_log_dir() / "t1.cancel.1.sig").exists()
    finally:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def test_cancel_reports_a_sig_write_that_never_landed(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    pid, pgid, ident = _orphan_sleep()
    try:
        _running_record(tmp_path, pid, pgid, ident)
        _flaky_sig(monkeypatch, failures=10_000)
        monkeypatch.setattr(ctl, "PHASE_WRITE_SETTLE_SECONDS", 1.0)

        assert ctl.main(["cancel", "t1", "--json"]) == 0

        captured = capsys.readouterr()
        result = json.loads(captured.out)["result"]
        assert result["unrecorded_phase_writes"] == ["t1 attempt 1 sig"]
        assert "could not all be recorded" in captured.err
    finally:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
