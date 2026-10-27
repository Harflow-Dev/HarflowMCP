from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

import pytest

from harflow_mcp import accounting
from harflow_mcp import store, task_documents
from harflow_mcp import state_ownership, tasks
from harflow_mcp.state_layout import resolve_layout


def test_normalize_preserves_zero_and_null_without_claiming_total() -> None:
    value = accounting.normalize(
        "task-1",
        {"input_tokens": 0, "output_tokens": 12, "cache_read_tokens": None},
        fields={"input_tokens": "input_tokens", "output_tokens": "output_tokens",
                "cache_read_tokens": "cache_read_tokens"},
        scope="execution",
        disjoint=True,
    )

    assert value["categories"] == {
        "input_tokens": 0,
        "output_tokens": 12,
        "cache_read_tokens": None,
        "cache_creation_tokens": None,
    }
    assert value["total_tokens"] is None
    assert value["additive"] is True


def test_normalize_computes_comparable_total_only_for_complete_disjoint_execution() -> None:
    usage = {
        "input_tokens": 7,
        "output_tokens": 11,
        "cache_read_tokens": 3,
        "cache_creation_tokens": 2,
    }

    comparable = accounting.normalize(
        "task-1", usage, fields={key: key for key in usage}, scope="execution", disjoint=True
    )
    overlapping = accounting.normalize(
        "task-2", usage, fields={key: key for key in usage}, scope="execution", disjoint=False
    )
    unknown_scope = accounting.normalize(
        "task-3", usage, fields={key: key for key in usage}, scope="unknown", disjoint=True
    )

    assert comparable["total_tokens"] == 23
    assert comparable["additive"] is True
    assert overlapping["total_tokens"] is None
    assert overlapping["additive"] is True
    assert unknown_scope["total_tokens"] is None
    assert unknown_scope["additive"] is False
    assert unknown_scope["categories"] == comparable["categories"]


def test_contribution_id_is_stable_per_execution() -> None:
    assert accounting.contribution_id("task-1") == "task:task-1"
    assert accounting.contribution_id("task-1") == accounting.contribution_id("task-1")
    assert accounting.contribution_id("task-2") != accounting.contribution_id("task-1")


def test_duration_unions_and_clamps_only_closed_qualified_intervals() -> None:
    result = accounting.duration(
        0,
        100,
        [
            {"start": 10, "end": 30, "execution_id": "task-1", "provenance": "caller_input", "qualified": True},
            {"start": 20, "end": 40, "execution_id": "task-1", "provenance": "approval", "qualified": True},
            {"start": -10, "end": 10, "execution_id": "task-1", "provenance": "caller_input", "qualified": True},
            {"start": 80, "end": 120, "execution_id": "task-1", "provenance": "caller_input", "qualified": True},
            {"start": 50, "end": None, "execution_id": "task-1", "provenance": "caller_input", "qualified": True},
            {"start": 60, "end": 70, "execution_id": "task-1", "provenance": "silence", "qualified": False},
        ],
    )

    assert result == {
        "elapsed_seconds": 100,
        "execution_seconds": 40,
        "wait_seconds": 60,
        "time_qualification": "qualified_waits",
    }


def test_duration_without_qualified_evidence_keeps_elapsed_time() -> None:
    result = accounting.duration(
        10,
        25,
        [{"start": 12, "end": 20, "execution_id": "task-1", "provenance": "pending_tool", "qualified": False}],
    )

    assert result["elapsed_seconds"] == 15
    assert result["execution_seconds"] == 15
    assert result["wait_seconds"] == 0
    assert result["time_qualification"] == "elapsed_may_include_waits"


def test_snapshot_failure_is_unavailable_and_unobserved_outcome_stays_unknown() -> None:
    class BrokenBackend:
        def accounting_evidence(self, acc, context):
            raise RuntimeError("adapter failed")

    broken = accounting.snapshot(
        BrokenBackend(), {}, {"task_id": "task-1", "observed": True, "terminal": True}
    )
    unobserved = accounting.snapshot(
        object(), {}, {"task_id": "task-2", "observed": False, "terminal": True}
    )

    assert broken["state"] == "unavailable"
    assert broken["diagnostics"] == ["accounting_evidence_error"]
    assert broken["total_tokens"] is None
    assert unobserved["state"] == "unavailable"
    assert unobserved["diagnostics"] == ["outcome_unobserved"]


def test_snapshot_marks_current_writer_pending_when_normalization_has_not_arrived() -> None:
    class PendingBackend:
        def accounting_evidence(self, acc, context):
            return accounting.unavailable(context["task_id"])

    pending = accounting.snapshot(
        PendingBackend(), {}, {"task_id": "task-1", "observed": True, "terminal": False}
    )

    assert pending["state"] == "pending"
    assert pending["additive"] is False
    assert pending["total_tokens"] is None


def test_validate_rejects_boolean_token_counts_and_duplicate_scope_claims() -> None:
    value = accounting.normalize(
        "task-1",
        {"input_tokens": 0, "output_tokens": 1, "cache_read_tokens": 2, "cache_creation_tokens": 3},
        fields={
            "input_tokens": "input_tokens",
            "output_tokens": "output_tokens",
            "cache_read_tokens": "cache_read_tokens",
            "cache_creation_tokens": "cache_creation_tokens",
        },
        scope="execution",
        disjoint=True,
    )
    malformed = {**value, "categories": {**value["categories"], "input_tokens": True}}

    try:
        accounting.validate(malformed)
    except ValueError:
        pass
    else:
        raise AssertionError("boolean must not be accepted as an integer token count")

    malformed_total = {**value, "total_tokens": 999}
    try:
        accounting.validate(malformed_total)
    except ValueError:
        pass
    else:
        raise AssertionError("a total must equal the complete category sum")


def test_malformed_unhashable_accounting_field_is_a_typed_task_document_refusal() -> None:
    record = store.TaskRecord(
        task_id="task-1",
        backend="vibe",
        session_id=None,
        repo_path="/repo",
        started_at="2027-01-01T00:00:00+00:00",
        accounting=accounting.unavailable("task-1"),
    )
    document = task_documents.encode(asdict(record))
    document["accounting"]["state"] = []

    with pytest.raises(task_documents.TaskDocumentError) as caught:
        task_documents.decode(document, "task-1", store.TaskRecord)

    assert caught.value.task_id == "task-1"
    assert caught.value.code == "task_incompatible"


def test_backend_accounting_lookup_failure_does_not_erase_landed_terminal_outcome(tmp_path, monkeypatch):
    layout = resolve_layout(root=tmp_path / "state")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        registry = tasks.TaskRegistry(log_dir=layout.tasks_directory)
        task = tasks.Task(
            task_id="task-1",
            backend="codex",
            session_id=None,
            repo_path=tmp_path,
            prompt="fixture",
            max_turns=1,
            log_path=layout.tasks_directory / "task-1.jsonl",
            started_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
            status="failed",
            exit_code=9,
            finished_at=datetime(2027, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
            enforcement=tasks.get_backend("codex").enforcement("write_in_repo").as_dict(),
        )

        def unavailable_backend(_name):
            raise RuntimeError("backend registry unavailable")

        monkeypatch.setattr(tasks, "get_backend", unavailable_backend)
        registry.persist(task)
        landed = store.read(layout.tasks_directory, "task-1")

    assert landed is not None
    assert (landed.status, landed.exit_code, landed.finished_at) == (
        "failed", 9, "2027-01-01T00:00:10+00:00"
    )
    assert landed.accounting["state"] == "unavailable"
    assert landed.accounting["diagnostics"] == ["accounting_evidence_error"]


def test_legacy_workflow_snapshot_without_accounting_remains_qualified():
    from harflow_mcp.workflow_run_documents import _snapshot_authority

    snapshot = {
        "enforcement": None,
        "owner": None,
        "outcome": None,
        "git_baseline": None,
    }

    _snapshot_authority(snapshot, "run")


def test_persist_with_missing_task_id_keeps_preexisting_invalid_id_behavior(tmp_path):
    layout = resolve_layout(root=tmp_path / "state")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        registry = tasks.TaskRegistry(log_dir=layout.tasks_directory)
        task = tasks.Task(
            task_id=None,
            backend="codex",
            session_id=None,
            repo_path=tmp_path,
            prompt="fixture",
            max_turns=1,
            log_path=layout.tasks_directory / "missing-id.jsonl",
            started_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        )

        registry.persist(task)

        assert task.latest_record_landed is False
        assert store.read_all(layout.tasks_directory) == []
