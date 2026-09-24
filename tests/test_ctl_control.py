"""`polybridge-ctl` control commands (A4.2): cancel, takeover, takeover-attach, and the run/resume
fork-and-handshake lifecycle. The handshake tests really fork: the child inherits this test's
monkeypatches (a fake backend, a temp HOME), owns the task, and is reaped here."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from polybridge import backends, control, ctl, detached, identity, lineage, store
from polybridge.backends import Enforcement, Invocation
from polybridge.tasks import default_log_dir

SESSION = "0199a3f2-7c1e-7b8a-9d0e-123456789abc"
CODEX_READ_ONLY = backends.get("codex").enforcement("read_only").as_dict()


class _FakeBackend:
    """Runs `/bin/sh -c <script>` — installed everywhere, and nothing here reads its output."""

    name = "fake"
    binary = "/bin/sh"
    capabilities = backends.get("claude").capabilities._replace(
        chooses_session_id=False, supports_live_input=False
    )

    def __init__(self, script: str = "sleep 0.3") -> None:
        self.script = script

    def build_start_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", self.script])

    def build_resume_argv(self, prompt, **kwargs):
        return Invocation([self.binary, "-c", self.script])

    def assert_safe(self, invocation, freedom, network=None):
        assert isinstance(invocation, Invocation)

    def encode_live_message(self, text):
        raise backends.UnsupportedCapability("no live input")

    def interactive_resume_argv(self, session_id, repo_path):
        return [self.binary, "--resume", session_id]

    def enforcement(self, freedom, network=None):
        return Enforcement(freedom=freedom, mechanism="none", os_enforced=False, writes_confined=False)

    def ingest(self, event, acc):
        return None

    def normalize(self, event, acc):
        return []

    def classify(self, acc, exit_code):
        return "completed" if exit_code == 0 else "failed"


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("PB_TASK_ID", raising=False)
    return tmp_path


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> _FakeBackend:
    backend = _FakeBackend()
    monkeypatch.setitem(backends.BACKENDS, backend.name, backend)
    return backend


@pytest.fixture
def outcomes(monkeypatch: pytest.MonkeyPatch):
    """Every `run_detached` outcome, so each forked child can be reaped once its task settles."""
    seen: list[detached.Outcome] = []
    real = detached.run_detached

    def recording(*args, **kwargs):
        outcome = real(*args, **kwargs)
        seen.append(outcome)
        return outcome

    monkeypatch.setattr(detached, "run_detached", recording)
    yield seen
    for outcome in seen:
        _reap(outcome.child_pid)


def _reap(pid: int, timeout: float = 20.0) -> int | None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            done, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return None
        if done:
            return os.waitstatus_to_exitcode(status)
        time.sleep(0.05)
    os.kill(pid, signal.SIGKILL)
    os.waitpid(pid, 0)
    raise AssertionError(f"child {pid} did not exit")


def _doc(capsys: pytest.CaptureFixture) -> dict:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1, lines
    return json.loads(lines[0])


def _settled(task_id: str, timeout: float = 15.0) -> store.TaskRecord:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = store.read(default_log_dir(), task_id)
        if record is not None and record.status in store.TERMINAL_RECORD_STATUSES:
            return record
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not settle")


# --- run / resume: the handshake ------------------------------------------------------------


def test_run_returns_the_task_id_and_the_child_owns_the_task_to_the_end(
    fake: _FakeBackend, git_repo: Path, outcomes, capsys: pytest.CaptureFixture
) -> None:
    code = ctl.main(
        ["run", "--backend", "fake", "--repo", str(git_repo), "--prompt", "hi", "--json"]
    )

    doc = _doc(capsys)
    assert code == 0
    assert doc["v"] == 1 and set(doc["result"]) == {"task_id"}
    task_id = doc["result"]["task_id"]
    record = _settled(task_id)
    assert record.status == "completed" and record.exit_code == 0
    # The owning process is the forked child, not this one.
    assert record.owner["pid"] == outcomes[0].child_pid
    assert _reap(outcomes[0].child_pid) == 0
    assert (default_log_dir().parent / "ctl.log").exists()


@pytest.mark.parametrize(
    ("argv_tail", "code"),
    [
        (["--backend", "nope", "--prompt", "hi"], "invalid_params"),
        (["--backend", "fake", "--prompt", "   "], "invalid_params"),
        (["--backend", "fake", "--prompt", "hi", "--freedom", "godmode"], "invalid_params"),
    ],
)
def test_run_reports_validation_errors_from_the_child(
    fake: _FakeBackend, git_repo: Path, outcomes, capsys: pytest.CaptureFixture, argv_tail, code
) -> None:
    exit_code = ctl.main(["run", "--repo", str(git_repo), *argv_tail, "--json"])

    doc = _doc(capsys)
    assert exit_code == 1
    assert doc == {"v": 1, "error": {"code": code, "message": doc["error"]["message"]}}
    assert outcomes[0].kind == "error"


def test_run_reports_a_bad_repo(fake: _FakeBackend, tmp_path: Path, outcomes, capsys) -> None:
    exit_code = ctl.main(
        ["run", "--backend", "fake", "--repo", str(tmp_path / "missing"), "--prompt", "hi", "--json"]
    )

    doc = _doc(capsys)
    assert exit_code == 1
    assert doc["error"]["code"] == "invalid_params"
    assert "does not exist" in doc["error"]["message"]


def test_run_applies_the_nested_dispatch_caps(
    fake: _FakeBackend, git_repo: Path, outcomes, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    parent = store.TaskRecord(
        task_id="parent",
        backend="codex",
        session_id="p",
        repo_path=str(git_repo),
        started_at=now,
        freedom="read_only",
        enforcement=CODEX_READ_ONLY,
        root_task_id="parent",
        max_depth=2,
    )
    monkeypatch.setattr(lineage, "detect_caller", lambda *a, **k: lineage.Caller(parent, "pb_task_id"))

    exit_code = ctl.main(
        ["run", "--backend", "fake", "--repo", str(git_repo), "--prompt", "hi", "--json"]
    )

    doc = _doc(capsys)
    assert exit_code == 1
    assert doc["error"]["code"] == "nested_dispatch_refused"
    assert store.read_all(default_log_dir()) == []


def test_resume_hands_back_a_new_task_on_the_same_session(
    fake: _FakeBackend, git_repo: Path, outcomes, capsys
) -> None:
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

    exit_code = ctl.main(["resume", "first", "go on", "--json"])

    doc = _doc(capsys)
    assert exit_code == 0
    record = _settled(doc["result"]["task_id"])
    assert record.session_id == SESSION and record.parent_task_id == "first"


def test_resume_errors_come_back_as_json(fake: _FakeBackend, outcomes, capsys) -> None:
    assert ctl.main(["resume", "nope", "go on", "--json"]) == 1
    assert _doc(capsys)["error"] == {"code": "invalid_params", "message": "unknown task_id: nope"}


def test_a_silent_child_is_reported_unknown_terminated_and_reaped(tmp_path: Path) -> None:
    async def hang(registry):
        await asyncio.sleep(3600)

    started = time.monotonic()
    outcome = detached.run_detached(
        hang, log_path=tmp_path / "ctl.log", registry_factory=lambda: None, timeout=1.0
    )

    assert outcome.kind == "unknown"
    assert "polybridge-ctl list" in outcome.payload["message"]
    assert time.monotonic() - started < 10
    with pytest.raises(ChildProcessError):
        os.waitpid(outcome.child_pid, os.WNOHANG)


def test_a_timed_out_child_cancels_the_task_it_already_started(
    fake: _FakeBackend, git_repo: Path, tmp_path: Path
) -> None:
    from polybridge.tasks import TaskRegistry

    fake.script = "sleep 30"
    log_dir = default_log_dir()

    async def start_then_hang(registry):
        await registry.start("hi", git_repo, backend=fake)
        await asyncio.sleep(3600)

    outcome = detached.run_detached(
        start_then_hang,
        log_path=tmp_path / "ctl.log",
        registry_factory=lambda: TaskRegistry(log_dir=log_dir, open_monitor=False),
        timeout=2.0,
    )

    assert outcome.kind == "unknown"
    (record,) = store.read_all(log_dir)
    assert record.status == "cancelled"
    assert identity.identity_check(
        identity.task_identity(record.pid, record.start_time, record.markers)
    ) == "dead"


def test_the_unknown_document_is_versioned_and_exits_3(
    monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    monkeypatch.setattr(
        detached,
        "run_detached",
        lambda *a, **k: detached.Outcome("unknown", {"message": "no word"}, 0),
    )

    assert ctl.main(["run", "--backend", "x", "--repo", "/", "--prompt", "p", "--json"]) == 3
    assert _doc(capsys) == {"v": 1, "unknown": {"message": "no word"}}


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
        dead_owner = {"pid": 2_000_000_001, "start_time": "x", "markers": []}
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
