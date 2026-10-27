from __future__ import annotations

import json

import pytest

from harflow_mcp import accounting
from harflow_mcp import state_ownership, store, takeover
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.statistics import StatisticsError, project, statistics_page


LOWER = 1_800_000_000
UPPER = 1_900_000_000


def source(kind: str, identity: str, payload: dict) -> dict:
    return {"key": f"{kind}:{identity}", "kind": kind, "id": identity,
            "witness": [1, 2, 3, 4, 5], "payload": payload, "error": None}


def run(run_id: str, *, created=LOWER + 10, finished=LOWER + 90, status="completed", parent_link=None, activations=None):
    return {"workflow_run_id": run_id, "created_at": created, "finished_at": finished,
            "finished_at_sequence": 3, "sequence": 3, "status": status, "repo_path": "/repo",
            "name": run_id, "workflow_id": "workflow-1", "revision": 2,
            "parent_link": parent_link, "activations": activations or []}


def task(task_id: str, *, started=UPPER + 10, finished=UPPER + 20, status="completed", **extra):
    return {"task_id": task_id, "backend": "vibe", "model": None, "repo_path": "/repo",
            "started_at": started, "finished_at": finished, "status": status,
            "accounting": accounting.unavailable(task_id, "fixture_unknown"), **extra}


def test_finished_root_owns_descendants_outside_the_selected_date_range() -> None:
    attempts = [{"task_id": "nested-task", "status": "completed", "started_at": UPPER + 10,
                 "finished_at": UPPER + 20}]
    child_run = run("child", created=UPPER + 1, finished=UPPER + 40,
                    parent_link={"workflow_run_id": "root", "root_workflow_run_id": "root"},
                    activations=[{"id": "activation-1", "node_id": "work", "role": "node", "tasks": attempts}])
    projection = project(
        [source("run", "root", run("root")), source("run", "child", child_run),
         source("task", "nested-task", task("nested-task"))],
        LOWER, UPPER,
    )

    executions = [item for item in projection["items"] if item["kind"] == "execution"]
    assert len(executions) == 1
    assert executions[0]["task_id"] == "nested-task"
    assert executions[0]["root_workflow_run_id"] == "root"
    assert executions[0]["cohort_started_at"] == LOWER + 10
    assert "task:nested-task" in projection["relevant_keys"]
    assert "run:child" in projection["relevant_keys"]


def test_workflow_completion_without_matching_sequence_witness_is_not_counted() -> None:
    stale = run("root")
    stale["sequence"] = 4
    task_attempt = {"task_id": "node-task", "status": "completed", "started_at": LOWER + 20,
                    "finished_at": LOWER + 30}
    stale["activations"] = [{"id": "a", "node_id": "work", "role": "node", "tasks": [task_attempt]}]

    projection = project(
        [source("run", "root", stale), source("task", "node-task", task("node-task"))],
        LOWER, UPPER,
    )

    assert projection["items"] == []
    assert projection["ownership_complete"] is False
    assert any(item["code"] == "completion_unavailable" for item in projection["diagnostics"])


def test_active_root_excludes_even_terminal_descendant_executions() -> None:
    active_root = run("root", status="running", finished=None)
    activation = {"id": "a", "node_id": "work", "role": "node", "tasks": [
        {"task_id": "finished-child", "status": "completed", "started_at": LOWER + 2, "finished_at": LOWER + 3}
    ]}
    active_root["activations"] = [activation]
    result = project([source("run", "root", active_root), source("task", "finished-child", task("finished-child", started=LOWER + 2, finished=LOWER + 3))], LOWER, UPPER)

    assert result["items"] == []


def test_finished_failure_and_cancellation_are_included_but_active_work_is_not() -> None:
    result = project(
        [source("task", "success", task("success", started=LOWER + 1, finished=LOWER + 2)),
         source("task", "failure", task("failure", status="failed", started=LOWER + 3, finished=LOWER + 4)),
         source("task", "cancelled", task("cancelled", status="cancelled", started=LOWER + 5, finished=LOWER + 6)),
         source("task", "active", task("active", status="running", started=LOWER + 7, finished=None))],
        LOWER, UPPER,
    )

    assert {item["task_id"] for item in result["items"] if item["kind"] == "execution"} == {
        "success", "failure", "cancelled"
    }


def test_standalone_resume_uses_its_own_cohort_and_keeps_out_of_range_ancestry() -> None:
    result = project(
        [source("task", "old-parent", task("old-parent", started=LOWER - 20, finished=LOWER - 10)),
         source("task", "resume", task("resume", started=LOWER + 1, finished=LOWER + 5,
                                         parent_task_id="old-parent", chain_root_task_id="old-parent"))],
        LOWER, UPPER,
    )

    assert [item["task_id"] for item in result["items"] if item["kind"] == "execution"] == ["resume"]
    assert result["items"][0]["parent_task_id"] == "old-parent"
    assert result["items"][0]["chain_root_task_id"] == "old-parent"
    assert "task:old-parent" in result["relevant_keys"]


def test_initial_retry_and_resume_are_distinct_executions_and_mismatched_wait_is_not_deducted() -> None:
    interval = {"start": LOWER + 2, "end": LOWER + 8, "execution_id": "different-task",
                "provenance": "caller_input", "qualified": True}
    result = project(
        [source("task", "initial", task("initial", started=LOWER + 1, finished=LOWER + 10)),
         source("task", "retry", task("retry", started=LOWER + 11, finished=LOWER + 20,
                                       parent_task_id="initial")),
         source("task", "resume", task("resume", started=LOWER + 21, finished=LOWER + 30,
                                        parent_task_id="retry", accounting={**accounting.unavailable("resume"),
                                                                          "qualified_wait_intervals": [interval]}))],
        LOWER, UPPER,
    )

    executions = [item for item in result["items"] if item["kind"] == "execution"]
    assert {item["task_id"] for item in executions} == {"initial", "retry", "resume"}
    assert sum(item["execution_kind"] == "resume" for item in executions) == 2
    resumed = next(item for item in executions if item["task_id"] == "resume")
    assert resumed["elapsed_seconds"] == 9
    assert resumed["wait_seconds"] == 0
    assert resumed["execution_seconds"] == 9


def test_unknown_parent_is_not_silently_reclassified_as_standalone() -> None:
    result = project(
        [source("task", "resume", task("resume", started=LOWER + 1, finished=LOWER + 5,
                                         parent_task_id="missing-parent"))],
        LOWER, UPPER,
    )

    assert result["items"] == []
    assert result["ownership_complete"] is False
    assert any(item["code"] == "ownership_unavailable" for item in result["diagnostics"])


def test_out_of_range_orphan_does_not_block_a_valid_in_range_standalone_task() -> None:
    result = project(
        [source("task", "orphan", task("orphan", started=LOWER - 20, finished=LOWER - 10,
                                         parent_task_id="missing-parent")),
         source("task", "valid", task("valid", started=LOWER + 1, finished=LOWER + 5))],
        LOWER, UPPER,
    )

    assert [item["task_id"] for item in result["items"] if item["kind"] == "execution"] == ["valid"]
    assert result["ownership_complete"] is True


def test_native_logical_execution_references_transport_without_adding_its_usage_twice() -> None:
    transport = task("transport", started=LOWER + 20, finished=LOWER + 30,
                     accounting=accounting.normalize("transport", {
                         "input": 5, "output": 4, "read": 0, "creation": 0,
                     }, fields={"input_tokens": "input", "output_tokens": "output",
                                "cache_read_tokens": "read", "cache_creation_tokens": "creation"},
                         scope="execution", disjoint=True))
    attempts = [
        {"task_id": "transport", "status": "completed", "started_at": LOWER + 20,
         "finished_at": LOWER + 30, "execution_kind": "native", "transport_task_id": "transport"},
        {"task_id": "logical-child", "status": "completed", "started_at": LOWER + 21,
         "finished_at": LOWER + 29, "execution_kind": "native", "transport_task_id": "transport",
         "candidate": {"backend": "vibe", "model": None}},
    ]
    root = run("root", activations=[{"id": "native-a", "node_id": "native", "role": "node", "tasks": attempts}])
    result = project([source("run", "root", root), source("task", "transport", transport)], LOWER, UPPER)

    executions = [item for item in result["items"] if item["kind"] == "execution"]
    assert len(executions) == 2
    assert all(item["execution_kind"] == "native" and not item["duration_additive"]
               and not item["usage_additive"] for item in executions)
    assert all(item["owner_task_id"] == "transport" for item in executions)
    assert all(item["accounting"]["total_tokens"] is None for item in executions)


def test_control_attempt_keeps_its_classification_and_requested_model_unknown() -> None:
    control_task = task("control", started=LOWER + 20, finished=LOWER + 30,
                        spawned_by="parent", model=None)
    root = run("root", activations=[{"id": "control-a", "node_id": "control", "role": "native_control",
                                    "tasks": [{"task_id": "control", "status": "completed",
                                               "started_at": LOWER + 20, "finished_at": LOWER + 30}]}])
    result = project([source("run", "root", root), source("task", "parent", task("parent", status="running")),
                      source("task", "control", control_task)], LOWER, UPPER)

    item = next(item for item in result["items"] if item.get("task_id") == "control")
    assert item["execution_kind"] == "control"
    assert item["requested_harness"] == "vibe"
    assert item["requested_model"] is None
    assert item["duration_additive"] is True
    assert item["usage_additive"] is False


@pytest.fixture
def owned_state(tmp_path, monkeypatch):
    layout = resolve_layout(root=tmp_path / "state")
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout


def persisted_task(task_id, *, status="completed", started="2027-01-01T00:00:00+00:00", title=None, **extra):
    finished = "2027-01-01T00:00:10+00:00" if status == "completed" else None
    return store.TaskRecord(
        task_id=task_id,
        backend="vibe",
        session_id=None,
        repo_path="/repo",
        started_at=started,
        status=status,
        finished_at=finished,
        title=title,
        model=None,
        **extra,
    )


def page(layout, cursor=None, limit=1):
    return statistics_page(
        layout,
        since="2026-12-31T00:00:00+00:00",
        before="2027-01-02T00:00:00+00:00",
        limit=limit,
        cursor=cursor,
    )


def test_requested_unknown_model_stays_null_and_legacy_accounting_is_unavailable(owned_state):
    store.write(owned_state.tasks_directory, persisted_task("task-1"))

    result = page(owned_state, limit=10)

    assert result["state"] == "ready"
    execution = next(item for item in result["items"] if item["kind"] == "execution")
    assert execution["requested_model"] is None
    assert execution["accounting"]["state"] == "unavailable"
    assert execution["accounting"]["total_tokens"] is None
    assert execution["duration_additive"] is True
    assert execution["usage_additive"] is False
    assert execution["execution_seconds"] == 10
    assert result["accounting_coverage"] == {"eligible_executions": 1, "available_executions": 0}
    assert result["retained_history_only"] is True
    assert len(json.dumps(result, allow_nan=False).encode()) <= 256 * 1024


def test_active_record_churn_does_not_expire_finished_history_cursor(owned_state):
    store.write(owned_state.tasks_directory, persisted_task("task-a"))
    store.write(owned_state.tasks_directory, persisted_task("task-b"))
    store.write(owned_state.tasks_directory, persisted_task("active", status="running"))
    first = page(owned_state)
    assert first["state"] == "ready" and first["has_more"]

    current = store.read(owned_state.tasks_directory, "active")
    store.write(owned_state.tasks_directory, store.replace(current, title="still running"))
    second = page(owned_state, cursor=first["next_cursor"])

    assert second["state"] == "ready"
    assert second["generation"] == first["generation"]
    assert second["has_more"] is False


def test_active_ancestry_metadata_churn_does_not_expire_cursor(owned_state):
    store.write(owned_state.tasks_directory, persisted_task("active-parent", status="running"))
    store.write(owned_state.tasks_directory, persisted_task("child", parent_task_id="active-parent"))
    store.write(owned_state.tasks_directory, persisted_task("independent"))
    first = page(owned_state)
    assert first["has_more"]

    parent = store.read(owned_state.tasks_directory, "active-parent")
    store.write(owned_state.tasks_directory, store.replace(parent, title="active progress changed"))
    second = page(owned_state, cursor=first["next_cursor"])

    assert second["state"] == "ready"
    assert second["generation"] == first["generation"]


@pytest.mark.parametrize("change", ["parent_identity", "terminal_transition"])
def test_active_ancestry_identity_or_terminal_transition_expires_cursor(owned_state, change):
    store.write(owned_state.tasks_directory, persisted_task("active-parent", status="running"))
    store.write(owned_state.tasks_directory, persisted_task("other-parent", status="running"))
    store.write(owned_state.tasks_directory, persisted_task("child", parent_task_id="active-parent"))
    store.write(owned_state.tasks_directory, persisted_task("independent"))
    first = page(owned_state)
    assert first["has_more"]

    parent = store.read(owned_state.tasks_directory, "active-parent")
    if change == "parent_identity":
        parent = store.replace(parent, parent_task_id="other-parent")
    else:
        parent = store.replace(parent, status="completed", finished_at="2027-01-01T00:00:20+00:00")
    store.write(owned_state.tasks_directory, parent)

    with pytest.raises(StatisticsError) as caught:
        page(owned_state, cursor=first["next_cursor"])
    assert caught.value.code == "snapshot_expired"


def test_relevant_terminal_task_mutation_expires_existing_cursor(owned_state):
    store.write(owned_state.tasks_directory, persisted_task("task-a"))
    store.write(owned_state.tasks_directory, persisted_task("task-b"))
    first = page(owned_state)
    assert first["has_more"]

    current = store.read(owned_state.tasks_directory, "task-b")
    store.write(owned_state.tasks_directory, store.replace(current, title="changed terminal evidence"))
    with pytest.raises(StatisticsError) as caught:
        page(owned_state, cursor=first["next_cursor"])

    assert caught.value.code == "snapshot_expired"


def test_unrelated_terminal_addition_outside_query_keeps_existing_cursor(owned_state):
    store.write(owned_state.tasks_directory, persisted_task("task-a"))
    store.write(owned_state.tasks_directory, persisted_task("task-b"))
    first = page(owned_state)
    assert first["has_more"]

    store.write(owned_state.tasks_directory, persisted_task(
        "outside", started="2026-12-01T00:00:00+00:00"
    ))
    second = page(owned_state, cursor=first["next_cursor"])

    assert second["state"] == "ready"
    assert second["generation"] == first["generation"]


def test_new_terminal_candidate_entering_query_expires_existing_cursor(owned_state):
    store.write(owned_state.tasks_directory, persisted_task("task-a"))
    store.write(owned_state.tasks_directory, persisted_task("task-b"))
    first = page(owned_state)
    assert first["has_more"]

    store.write(owned_state.tasks_directory, persisted_task("new-in-range"))
    with pytest.raises(StatisticsError) as caught:
        page(owned_state, cursor=first["next_cursor"])

    assert caught.value.code == "snapshot_expired"


def test_caller_refusal_happens_before_statistics_index_access(owned_state, monkeypatch):
    from harflow_mcp import statistics

    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: ("agent_caller", "agent identity"))

    class MustNotOpen:
        def __init__(self, *_args, **_kwargs):
            raise AssertionError("statistics index must not be opened for a refused caller")

    monkeypatch.setattr(statistics, "StatisticsIndex", MustNotOpen)
    with pytest.raises(StatisticsError) as caught:
        page(owned_state)
    assert caught.value.code == "agent_caller"


def test_retention_disclosure_distinguishes_disabled_and_unknown(owned_state, monkeypatch):
    from harflow_mcp import retention

    monkeypatch.delenv("HARFLOW_RETENTION_DAYS", raising=False)
    assert page(owned_state)["retention"] == {"state": "days", "days": 30}

    monkeypatch.setenv("HARFLOW_RETENTION_DAYS", "0")
    monkeypatch.setattr(retention, "retention_days", lambda: None)
    disabled = page(owned_state)
    assert disabled["retention"] == {"state": "disabled", "days": None}

    monkeypatch.setenv("HARFLOW_RETENTION_DAYS", "invalid")
    unknown = page(owned_state)
    assert unknown["retention"] == {"state": "unknown", "days": None}


def test_ctl_statistics_page_returns_the_v1_envelope(tmp_path, monkeypatch, capsys):
    from harflow_mcp import ctl

    monkeypatch.setenv("HOME", str(tmp_path))
    layout = resolve_layout(root=tmp_path / ".harflow")
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        store.write(layout.tasks_directory, persisted_task("task-1"))
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)

    code = ctl.main([
        "statistics-page",
        "--since", "2026-12-31T00:00:00+00:00",
        "--before", "2027-01-02T00:00:00+00:00",
        "--limit", "10",
        "--json",
    ])

    assert code == 0
    document = json.loads(capsys.readouterr().out)
    assert document["format"] == "harflow.ctl"
    assert document["v"] == 1
    assert document["result"]["state"] == "ready"
    assert document["result"]["items"]
