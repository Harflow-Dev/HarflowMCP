"""On-disk task records, which let a restarted server still see earlier tasks."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from polybridge import store
from polybridge.store import TaskRecord

SESSION = "0b4147f7-3a0e-4fe1-8399-d8c9964ccb2e"

RESULT_EVENT = {
    "type": "result",
    "subtype": "success",
    "session_id": SESSION,
    "result": "Created hello.txt.",
    "is_error": False,
    "num_turns": 2,
    "total_cost_usd": 0.25,
    "permission_denials": [],
}


def make_record(**overrides) -> TaskRecord:
    base = {
        "task_id": "task-1",
        "backend": "claude",
        "session_id": SESSION,
        "markers": [SESSION],
        "repo_path": "/tmp/repo",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "pid": None,
        "pgid": None,
        "model": "sonnet",
        "max_turns": 20,
        "prompt": "do a thing",
    }
    return TaskRecord(**(base | overrides))


def write_log(log_dir: Path, task_id: str, *events: dict) -> None:
    path = store.log_path(log_dir, task_id)
    path.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")


def test_round_trips_a_record(tmp_path: Path) -> None:
    record = make_record()
    store.write(tmp_path, record)

    assert store.read(tmp_path, "task-1") == record


def test_missing_record_reads_as_none(tmp_path: Path) -> None:
    assert store.read(tmp_path, "nope") is None


def test_unknown_fields_are_ignored_so_newer_records_still_load(tmp_path: Path) -> None:
    """A record written by a future version must not break an older server."""
    store.write(tmp_path, make_record())
    path = store.record_path(tmp_path, "task-1")
    raw = json.loads(path.read_text())
    raw["something_from_the_future"] = 42
    path.write_text(json.dumps(raw), encoding="utf-8")

    assert store.read(tmp_path, "task-1") is not None


@pytest.mark.parametrize("contents", ["{not json", '"a string"', ""])
def test_unreadable_records_are_skipped_not_raised(tmp_path: Path, contents: str) -> None:
    store.record_path(tmp_path, "task-1").write_text(contents, encoding="utf-8")

    assert store.read(tmp_path, "task-1") is None


def test_read_all_is_ordered_by_start_time(tmp_path: Path) -> None:
    store.write(tmp_path, make_record(task_id="second", started_at="2026-07-29T10:00:01+00:00"))
    store.write(tmp_path, make_record(task_id="first", started_at="2026-07-29T10:00:00+00:00"))

    assert [r.task_id for r in store.read_all(tmp_path)] == ["first", "second"]


def test_writing_a_record_never_raises_on_a_bad_directory(tmp_path: Path) -> None:
    """Losing a record must not fail the dispatch it belongs to."""
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("x")

    store.write(blocker, make_record())


@pytest.mark.parametrize("pid", [None, 999_999_999])
def test_process_alive_is_false_for_missing_processes(pid: int | None) -> None:
    assert store.process_alive(pid, [SESSION]) is False


def test_process_alive_rejects_a_reused_pid() -> None:
    """Our own pid is alive but is not a claude run carrying this session id."""
    assert store.process_alive(os.getpid(), [SESSION]) is False


def test_recovers_a_finished_run_from_its_own_output(tmp_path: Path) -> None:
    """The server that started it died before recording the outcome; the log still has it."""
    record = make_record(pid=999_999_999)
    write_log(tmp_path, record.task_id, {"type": "system", "subtype": "init"}, RESULT_EVENT)

    snap = store.snapshot(tmp_path, record)

    assert snap["status"] == "completed"
    assert snap["summary"] == "Created hello.txt."
    assert snap["num_turns"] == 2
    assert snap["total_cost_usd"] == pytest.approx(0.25)
    assert snap["recovered"] is True
    assert "reconstructed" in snap["note"]


def test_recovers_max_turns_exhaustion_as_timed_out(tmp_path: Path) -> None:
    record = make_record(pid=999_999_999)
    write_log(tmp_path, record.task_id, RESULT_EVENT | {"subtype": "error_max_turns"})

    assert store.snapshot(tmp_path, record)["status"] == "timed_out"


def test_a_gone_process_with_no_result_reads_as_interrupted(tmp_path: Path) -> None:
    record = make_record(pid=999_999_999)
    write_log(tmp_path, record.task_id, {"type": "system", "subtype": "init"})

    snap = store.snapshot(tmp_path, record)

    assert snap["status"] == "failed"
    assert "interrupted" in snap["note"]
    # The distinction that matters to a caller: its edits were not rolled back.
    assert "undone" in snap["note"]


def test_a_recorded_terminal_status_is_trusted(tmp_path: Path) -> None:
    record = make_record(status="cancelled", exit_code=143, finished_at="2026-07-29T10:05:00+00:00")

    assert store.snapshot(tmp_path, record)["status"] == "cancelled"


def test_a_missing_log_does_not_break_recovery(tmp_path: Path) -> None:
    snap = store.snapshot(tmp_path, make_record(pid=999_999_999))

    assert snap["status"] == "failed"
    assert snap["summary"] is None
    assert snap["last_output_tail"] == []


def test_recovered_snapshot_matches_the_live_snapshot_shape(tmp_path: Path) -> None:
    """Callers must not need to special-case a recovered task, so the keys line up."""
    from polybridge.tasks import Task

    live = Task(
        task_id="t",
        backend="claude",
        session_id=SESSION,
        repo_path=tmp_path,
        prompt="x",
        max_turns=5,
        log_path=tmp_path / "t.jsonl",
        started_at=datetime.now(timezone.utc),
    ).snapshot()

    recovered = store.snapshot(tmp_path, make_record(pid=999_999_999))

    assert set(live) - set(recovered) == set()
    # Recovery adds provenance the live path has no need for.
    assert set(recovered) - set(live) == {"recovered", "note"}


def test_brief_reports_liveness(tmp_path: Path) -> None:
    entry = store.brief(tmp_path, make_record(pid=999_999_999))

    assert entry["status"] == "failed"
    assert entry["recovered"] is True
    assert entry["session_id"] == SESSION


def test_long_prompts_are_truncated_in_the_record() -> None:
    assert store.PROMPT_PREVIEW_CHARS < 10_000


@pytest.mark.parametrize(
    "task_id",
    [
        pytest.param("../../../etc/passwd", id="traversal"),
        pytest.param("..", id="parent"),
        pytest.param("a/b", id="separator"),
        pytest.param("", id="empty"),
        pytest.param(".hidden", id="leading-dot"),
        pytest.param("x" * 200, id="too-long"),
    ],
)
def test_task_ids_that_could_escape_the_directory_are_rejected(
    tmp_path: Path, task_id: str
) -> None:
    """The id reaches us from a caller and becomes a file path."""
    with pytest.raises(store.InvalidTaskId):
        store.record_path(tmp_path, task_id)
    with pytest.raises(store.InvalidTaskId):
        store.log_path(tmp_path, task_id)


def test_a_real_uuid_is_accepted(tmp_path: Path) -> None:
    assert store.record_path(tmp_path, "6d06583d-4219-4aeb-af05-0dd67e89688e").parent == tmp_path


def test_an_invalid_id_is_not_written_and_does_not_raise(tmp_path: Path) -> None:
    store.write(tmp_path, make_record(task_id="../escape"))

    assert list(tmp_path.iterdir()) == []


def test_a_stale_running_record_cannot_overwrite_a_recorded_outcome(tmp_path: Path) -> None:
    """Several server processes may write the same record; a task must not move backwards."""
    store.write(tmp_path, make_record(status="completed", exit_code=0))

    store.write(tmp_path, make_record(status="running"))

    assert store.read(tmp_path, "task-1").status == "completed"


def test_a_terminal_status_may_still_be_corrected_to_another_terminal_one(tmp_path: Path) -> None:
    store.write(tmp_path, make_record(status="running"))
    store.write(tmp_path, make_record(status="cancelled"))

    assert store.read(tmp_path, "task-1").status == "cancelled"


def test_listing_and_status_never_disagree(tmp_path: Path) -> None:
    """A task reported one way by get_task_status must not be listed as something else."""
    record = make_record(pid=999_999_999)
    write_log(tmp_path, record.task_id, RESULT_EVENT)

    assert store.brief(tmp_path, record)["status"] == store.snapshot(tmp_path, record)["status"]
    assert store.snapshot(tmp_path, record)["status"] == "completed"


def test_unidentifiable_but_live_process_is_treated_as_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ps cannot be run, inventing an outcome is worse than reporting it as still going."""

    def no_ps(*args, **kwargs):
        raise OSError("ps unavailable")

    monkeypatch.setattr(store.subprocess, "run", no_ps)

    assert store.process_alive(os.getpid(), [SESSION]) is True


def test_live_session_ids_spans_processes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(store, "process_alive", lambda pid, markers: pid == 1)
    store.write(tmp_path, make_record(task_id="live", session_id="s-live", pid=1))
    store.write(tmp_path, make_record(task_id="dead", session_id="s-dead", pid=2))

    assert store.live_session_ids(tmp_path) == {"s-live"}
