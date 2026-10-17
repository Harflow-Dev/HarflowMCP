"""A1: one metadata scan for caller detection, fail-closed preserved.

`lineage.read_candidates` lets a call site that needs both `detect_caller` and
`detect_caller_detail` read the task records once instead of three times, without weakening the
gate: a failed read must produce exactly the undecidable refusal the scanning path produced, from
`detect_caller_detail` (always unstubbed), never silently from a stubbed `detect_caller`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from polybridge import ctl, lineage, server, store
from polybridge.lineage import detect_caller as real_detect_caller
from polybridge.lineage import detect_caller_detail as real_detect_caller_detail
from polybridge.tasks import TaskRegistry


@pytest.fixture(autouse=True)
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    return tmp_path


def _log_dir(home: Path) -> Path:
    from polybridge.tasks import default_log_dir

    return default_log_dir()


def _use_real_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """Undo the conftest stubs, so the real (but record-free) detection runs."""
    monkeypatch.setattr(lineage, "detect_caller", real_detect_caller)
    monkeypatch.setattr(lineage, "detect_caller_detail", real_detect_caller_detail)


def _make_record(**overrides) -> store.TaskRecord:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    base = {
        "task_id": "task-1",
        "backend": "claude",
        "session_id": "s1",
        "markers": ["s1"],
        "repo_path": "/tmp/repo",
        "started_at": now,
        "status": "completed",
        "exit_code": 0,
        "finished_at": now,
        "owner": {"pid": 4242, "start_time": "Wed Jan 1 00:00:00 2000", "markers": []},
    }
    return store.TaskRecord(**(base | overrides))


def test_read_candidates_never_raises_and_the_sentinel_fails_closed(monkeypatch, tmp_path):
    directory = tmp_path / "tasks"
    monkeypatch.setattr(store, "read_all", lambda log_dir: (_ for _ in ()).throw(ValueError("boom")))
    result = lineage.read_candidates(directory).resolve()
    assert isinstance(result, lineage._CandidateReadFailure)
    assert real_detect_caller(directory, candidates=result) is None
    detail = real_detect_caller_detail(directory, candidates=result)
    assert detail.caller is None
    assert detail.undecidable == "caller detection failed: ValueError: boom"


def test_sentinel_message_is_byte_identical_to_the_scanning_failure(monkeypatch, tmp_path):
    directory = tmp_path / "tasks"
    monkeypatch.setattr(store, "read_all", lambda log_dir: (_ for _ in ()).throw(OSError("denied")))
    # Today's path: the scan raises inside each function and the failure message is built there.
    assert real_detect_caller(directory) is None
    scanning = real_detect_caller_detail(directory)
    sentinel = real_detect_caller_detail(directory, candidates=lineage.read_candidates(directory))
    assert sentinel == scanning
    assert sentinel.undecidable == "caller detection failed: OSError: denied"


def test_read_candidates_success_is_reused_by_both_phases(monkeypatch, tmp_path):
    store.write(tmp_path / "tasks", _make_record())
    candidates = lineage.read_candidates(tmp_path / "tasks").resolve()
    # Terminal-with-exit-code records are not caller candidates at all.
    assert candidates == []
    assert real_detect_caller(tmp_path / "tasks", candidates=candidates) is None
    assert real_detect_caller_detail(tmp_path / "tasks", candidates=candidates) == lineage.Detection(None)


def test_ctl_status_scans_the_metadata_once(monkeypatch, home, capsys):
    store.write(_log_dir(home), _make_record())
    _use_real_detection(monkeypatch)
    real_read_all = store.read_all
    calls = []

    def counting(log_dir):
        calls.append(log_dir)
        return real_read_all(log_dir)

    monkeypatch.setattr(store, "read_all", counting)
    assert ctl.main(["status", "task-1", "--json"]) == 0
    assert json_document(capsys)["task"]["task_id"] == "task-1"
    assert len(calls) == 1


async def test_server_workflow_read_scans_the_metadata_once(monkeypatch, tmp_path):
    directory = tmp_path / "tasks"
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=directory))
    _use_real_detection(monkeypatch)
    real_read_all = store.read_all
    calls = []

    def counting(log_dir):
        calls.append(log_dir)
        return real_read_all(log_dir)

    monkeypatch.setattr(store, "read_all", counting)
    assert await server._managed_workflow_reader() is None
    assert len(calls) == 1


def _fail_read_all(monkeypatch, exc=ValueError("boom")):
    def boom(log_dir):
        raise exc

    monkeypatch.setattr(store, "read_all", boom)


def test_ctl_status_read_failure_refuses_like_today(monkeypatch, home, capsys):
    """A stubbed `detect_caller` ignores the sentinel; the undecidable comes from the detail gate.

    The conftest stub for `detect_caller` stays in place here on purpose: the plan pins that a
    stub cannot turn a failed read into "no caller", only `detect_caller_detail` (always
    unstubbed) may answer, and it must fail closed.
    """
    monkeypatch.setattr(lineage, "detect_caller_detail", real_detect_caller_detail)
    _fail_read_all(monkeypatch)
    assert ctl.main(["status", "task-1", "--json"]) == 1
    doc = json_document(capsys)
    assert doc["error"]["code"] == "workflow_read_refused"
    assert doc["error"]["message"] == (
        "Workflow caller authority is undecidable: caller detection failed: ValueError: boom"
    )


async def test_server_read_failure_refuses_like_today(monkeypatch, tmp_path):
    from mcp import MCPError

    directory = tmp_path / "tasks"
    monkeypatch.setattr(server, "_registry", TaskRegistry(log_dir=directory))
    monkeypatch.setattr(lineage, "detect_caller_detail", real_detect_caller_detail)
    _fail_read_all(monkeypatch, OSError("unreadable"))
    with pytest.raises(MCPError, match="Workflow caller authority is undecidable: caller detection failed: OSError: unreadable"):
        await server._managed_workflow_reader()


def test_injected_caller_never_scans(monkeypatch, tmp_path):
    """`_caller_override` short-circuits before any candidate read."""
    import asyncio

    marker = object()
    monkeypatch.setattr(store, "read_all", lambda log_dir: (_ for _ in ()).throw(AssertionError("must not scan")))
    registry = TaskRegistry(log_dir=tmp_path / "tasks", caller_override=marker)
    assert asyncio.run(registry._detect_caller()) is marker


def json_document(capsys):
    import json

    return json.loads(capsys.readouterr().out)
