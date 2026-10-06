"""`polybridge-ctl`: the CLI over on-disk task records (read-only, except `send`)."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from mcp import MCPError

from conftest import ALIVE_OWNER

from polybridge import ctl, identity, inbox, store
from polybridge.tasks import TaskRegistry


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def make_record(**overrides) -> store.TaskRecord:
    now = datetime.now(timezone.utc)
    base = {
        "task_id": "task-1",
        "backend": "claude",
        "session_id": "s1",
        "markers": ["s1"],
        "repo_path": "/tmp/repo",
        "started_at": _iso(now),
        "status": "completed",
        "exit_code": 0,
        "finished_at": _iso(now),
        "owner": {"pid": 4242, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []},
    }
    return store.TaskRecord(**(base | overrides))


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """`ctl` reads `tasks.default_log_dir()`, which is `Path.home() / ".polybridge" / "tasks"` —
    redirect HOME so nothing here ever touches the real `~/.polybridge`."""
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _log_dir(home: Path) -> Path:
    from polybridge.tasks import default_log_dir

    return default_log_dir()


def test_list_json_is_exactly_one_document(home: Path, capsys: pytest.CaptureFixture) -> None:
    store.write(_log_dir(home), make_record())

    code = ctl.main(["list", "--json"])

    assert code == 0
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 1
    doc = json.loads(lines[0])
    assert doc["v"] == 6
    assert len(doc["tasks"]) == 1
    assert doc["tasks"][0]["task_id"] == "task-1"
    assert doc["tasks"][0]["owner"]["pid"] == 4242


def test_list_json_with_no_tasks_is_still_one_document(
    home: Path, capsys: pytest.CaptureFixture
) -> None:
    code = ctl.main(["list", "--json"])

    assert code == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc == {"v": 6, "tasks": []}


def test_list_plain_table_names_its_columns(home: Path, capsys: pytest.CaptureFixture) -> None:
    store.write(_log_dir(home), make_record())

    code = ctl.main(["list"])

    assert code == 0
    out = capsys.readouterr().out
    assert "task_id" in out
    assert "task-1" in out
    assert "4242" in out  # owner pid


def test_list_since_filters_out_older_tasks(home: Path, capsys: pytest.CaptureFixture) -> None:
    now = datetime.now(timezone.utc)
    log_dir = _log_dir(home)
    store.write(log_dir, make_record(task_id="recent", started_at=_iso(now - timedelta(hours=1))))
    store.write(log_dir, make_record(task_id="old", started_at=_iso(now - timedelta(days=30))))

    code = ctl.main(["list", "--since", "1d", "--json"])

    assert code == 0
    doc = json.loads(capsys.readouterr().out)
    task_ids = {t["task_id"] for t in doc["tasks"]}
    assert task_ids == {"recent"}


def test_a_bad_since_value_is_a_usage_error(home: Path, capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as exc_info:
        ctl.main(["list", "--since", "not-a-duration", "--json"])

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    doc = json.loads(captured.out)
    assert doc["v"] == 6
    assert doc["error"]["code"] == "usage"
    assert captured.err.strip()


def test_status_json_for_a_known_task(home: Path, capsys: pytest.CaptureFixture) -> None:
    store.write(_log_dir(home), make_record())

    code = ctl.main(["status", "task-1", "--json"])

    assert code == 0
    out = capsys.readouterr().out
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 1
    doc = json.loads(lines[0])
    assert doc["v"] == 6
    assert doc["task"]["task_id"] == "task-1"
    assert doc["task"]["status"] == "completed"


def test_status_json_for_an_unknown_task(home: Path, capsys: pytest.CaptureFixture) -> None:
    code = ctl.main(["status", "no-such-task", "--json"])

    assert code == 1
    captured = capsys.readouterr()
    doc = json.loads(captured.out)
    assert doc == {
        "v": 6,
        "error": {"code": "unknown_task", "message": "unknown task_id: no-such-task"},
    }
    assert "no-such-task" in captured.err


def test_status_json_for_an_invalid_task_id(home: Path, capsys: pytest.CaptureFixture) -> None:
    code = ctl.main(["status", "../escape", "--json"])

    assert code == 1
    doc = json.loads(capsys.readouterr().out)
    assert doc["error"]["code"] == "invalid_task_id"


def test_status_plain_text_for_an_unknown_task_prints_nothing_to_stdout(
    home: Path, capsys: pytest.CaptureFixture
) -> None:
    code = ctl.main(["status", "no-such-task"])

    assert code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "no-such-task" in captured.err


def test_bad_subcommand_is_a_usage_error(home: Path, capsys: pytest.CaptureFixture) -> None:
    with pytest.raises(SystemExit) as exc_info:
        ctl.main(["bogus"])

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.strip()


def test_bad_subcommand_with_json_still_emits_a_usage_document(
    home: Path, capsys: pytest.CaptureFixture
) -> None:
    with pytest.raises(SystemExit) as exc_info:
        ctl.main(["bogus", "--json"])

    assert exc_info.value.code == 2
    doc = json.loads(capsys.readouterr().out)
    assert doc["v"] == 6
    assert doc["error"]["code"] == "usage"


def test_ctl_never_constructs_a_task_registry(
    home: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """ctl is read-only: it must not start retention or hold a live registry."""
    from polybridge import tasks as tasks_module

    def boom(*args, **kwargs):
        raise AssertionError("polybridge-ctl must never construct a TaskRegistry")

    monkeypatch.setattr(tasks_module, "TaskRegistry", boom)
    store.write(_log_dir(home), make_record())

    assert ctl.main(["list", "--json"]) == 0
    assert ctl.main(["status", "task-1", "--json"]) == 0


# --- resume: the still-running guard ------------------------------------------------------------


def _run_resume_guard(log_dir: Path, task_id: str) -> None:
    """Runs `ctl resume`'s action in-process, up to and including its guard.

    The real command forks (`detached.run_detached`), and the fork is tested from a fresh
    interpreter (`ctl_driver.py`); the guard itself is a read-and-check against the record, so
    it runs here. Refusal raises the same MCPError the forked child would report back.
    """
    args = argparse.Namespace(task_id=task_id, text="go on", max_turns=None, network=None)
    action = ctl._resume_action(args)
    registry = TaskRegistry(log_dir=log_dir, open_monitor=False)
    asyncio.run(action(registry))


def test_resume_refuses_a_retitled_live_run(home: Path) -> None:
    """`ctl resume` applies the same gate `resume_task` does: a live vibe run renamed itself to
    `Vibe CLI`, so its markers never match the command line again — but its start time does, so
    the resume must be refused rather than started against a session still being written to."""
    captured = identity.capture(os.getpid(), [])
    assert captured is not None
    store.write(
        _log_dir(home),
        make_record(
            pid=os.getpid(),
            start_time=captured["start_time"],
            markers=["definitely-not-in-the-cmdline"],
        ),
    )

    with pytest.raises(MCPError, match="still running"):
        _run_resume_guard(_log_dir(home), "task-1")


def test_resume_refuses_when_ps_fails(home: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`ps_failed` is undecidable, not dead — uncertainty must block a resume, exactly as on the
    server's `resume_task` guard."""
    monkeypatch.setattr(identity, "_run_ps", lambda pid: None)
    store.write(
        _log_dir(home),
        make_record(pid=4321, start_time="Wed Jan  1 00:00:00 2000", markers=["s1"]),
    )

    with pytest.raises(MCPError, match="still running"):
        _run_resume_guard(_log_dir(home), "task-1")


# --- send (live input) ------------------------------------------------------------------------


def _live_running(**overrides) -> store.TaskRecord:
    fields = {
        "status": "running",
        "exit_code": None,
        "finished_at": None,
        "live_input": True,
        "owner": ALIVE_OWNER,
    }
    return make_record(**(fields | overrides))


def _one_doc(capsys: pytest.CaptureFixture) -> dict:
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert len(lines) == 1
    return json.loads(lines[0])


def test_send_queues_a_message_in_the_inbox(
    identities, home: Path, capsys: pytest.CaptureFixture) -> None:
    store.write(_log_dir(home), _live_running())

    code = ctl.main(["send", "task-1", "please also check the tests", "--json"])

    assert code == 0
    doc = _one_doc(capsys)
    assert doc["v"] == 6
    assert doc["result"]["status"] == "queued"
    messages, _ = inbox.read_new(_log_dir(home), "task-1", 0)
    assert [m["text"] for m in messages] == ["please also check the tests"]
    assert messages[0]["id"] == doc["result"]["message_id"]


def test_send_after_close_says_to_resume(
    identities, home: Path, capsys: pytest.CaptureFixture) -> None:
    store.write(_log_dir(home), _live_running())
    inbox.mark_closed(_log_dir(home), "task-1")

    code = ctl.main(["send", "task-1", "hi", "--json"])

    assert code == 1
    doc = _one_doc(capsys)
    assert doc["error"]["code"] == "closed"
    assert "finished; continue with resume_task" in doc["error"]["message"]


@pytest.mark.parametrize(
    ("record", "code"),
    [
        (lambda: make_record(live_input=True), "settled"),
        (lambda: _live_running(live_input=False), "not_live_input"),
        (lambda: _live_running(owner=None), "owner_not_alive"),
    ],
    ids=["settled", "not-live", "no-owner"],
)
def test_send_refusals_are_json_errors(
    identities, home: Path, capsys: pytest.CaptureFixture, record, code) -> None:
    store.write(_log_dir(home), record())

    assert ctl.main(["send", "task-1", "hi", "--json"]) == 1
    assert _one_doc(capsys)["error"]["code"] == code


def test_send_rejects_bad_ids_unknown_tasks_and_empty_text(
    identities, home: Path, capsys: pytest.CaptureFixture
) -> None:
    assert ctl.main(["send", "../x", "hi", "--json"]) == 1
    assert _one_doc(capsys)["error"]["code"] == "invalid_task_id"
    assert ctl.main(["send", "nope", "hi", "--json"]) == 1
    assert _one_doc(capsys)["error"]["code"] == "unknown_task"
    store.write(_log_dir(home), _live_running())
    assert ctl.main(["send", "task-1", "   ", "--json"]) == 1
    assert _one_doc(capsys)["error"]["code"] == "empty_text"


def test_send_without_json_prints_a_line(
    identities, home: Path, capsys: pytest.CaptureFixture) -> None:
    store.write(_log_dir(home), _live_running())
    assert ctl.main(["send", "task-1", "hi"]) == 0
    assert "queued" in capsys.readouterr().out


# --- backends (registry + PATH presence) --------------------------------------------------------


def test_backends_json_reports_the_registry_in_order(
    home: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from polybridge import backends as backends_module

    claude_binary = backends_module.BACKENDS["claude"].binary
    monkeypatch.setattr(
        backends_module.shutil,
        "which",
        lambda binary: None if binary == claude_binary else f"/usr/local/bin/{binary}",
    )

    assert ctl.main(["backends", "--json"]) == 0

    doc = _one_doc(capsys)
    assert doc["v"] == 6
    assert [b["backend"] for b in doc["backends"]] == [
        "claude", "codex", "opencode", "vibe", "antigravity"
    ]
    assert doc["backends"] == [
        {"backend": name, "binary": backend.binary, "installed": name != "claude"}
        for name, backend in backends_module.BACKENDS.items()
    ]


def test_backends_plain_text_is_one_line_per_backend(
    home: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    from polybridge import backends as backends_module

    vibe_binary = backends_module.BACKENDS["vibe"].binary
    monkeypatch.setattr(
        backends_module.shutil,
        "which",
        lambda binary: f"/usr/local/bin/{binary}" if binary == vibe_binary else None,
    )

    assert ctl.main(["backends"]) == 0

    assert capsys.readouterr().out.splitlines() == [
        "claude  not found on PATH",
        "codex  not found on PATH",
        "opencode  not found on PATH",
        "vibe  installed",
        "antigravity  not found on PATH",
    ]


def test_backends_never_spawns_a_subprocess(
    home: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """`backends` answers from `shutil.which` alone — unlike `describe()`, it never probes
    `--version`, so it stays fast enough for the Monitor's refresh cadence."""

    from polybridge import backends as backends_module

    def boom(*args, **kwargs):
        raise AssertionError("ctl backends must never spawn a subprocess")

    monkeypatch.setattr(backends_module.subprocess, "run", boom)
    monkeypatch.setattr(backends_module.subprocess, "Popen", boom)

    assert ctl.main(["backends", "--json"]) == 0
    assert ctl.main(["backends"]) == 0
