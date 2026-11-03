"""Statistics cost and observed model: record fields, projection facts and coverage."""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone

import pytest

from harflow_mcp import state_ownership, store, takeover, tasks
from harflow_mcp.backends.base import Accumulator
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends.opencode import OpencodeBackend
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.statistics import project, statistics_page
from harflow_mcp.wire_contracts import CAPABILITIES

LOWER = 1_800_000_000
UPPER = 1_900_000_000


def test_capability_is_declared():
    assert "statistics-cost-model.v1" in CAPABILITIES


def init(model, version="2.1.0"):
    return {"type": "system", "subtype": "init", "model": model, "claude_code_version": version, "session_id": "s"}


def result(cost):
    return {"type": "result", "subtype": "success", "is_error": False, "result": "ok", "session_id": "s",
            "num_turns": 1, "total_cost_usd": cost, "usage": {"input_tokens": 3, "output_tokens": 4}, "permission_denials": []}


def feed(backend, events) -> Accumulator:
    acc = Accumulator()
    for event in events:
        backend.ingest(json.loads(json.dumps(event)), acc)
    return acc


@pytest.fixture
def owned(tmp_path, monkeypatch):
    layout = resolve_layout(root=tmp_path / "state")
    monkeypatch.setattr(takeover, "caller_refusal", lambda *_: None)
    with state_ownership.state_session(layout):
        layout.tasks_directory.mkdir(parents=True)
        yield layout


def persisted(layout, acc, *, backend="claude", task_id="task-1"):
    registry = tasks.TaskRegistry(log_dir=layout.tasks_directory)
    task = tasks.Task(task_id=task_id, backend=backend, session_id=None, repo_path=layout.root, prompt="fixture",
                      max_turns=1, log_path=layout.tasks_directory / f"{task_id}.jsonl",
                      started_at=datetime(2027, 1, 1, tzinfo=timezone.utc), status="completed", exit_code=0,
                      finished_at=datetime(2027, 1, 1, 0, 0, 10, tzinfo=timezone.utc),
                      enforcement=tasks.get_backend(backend).enforcement("write_in_repo").as_dict())
    task.acc = acc
    registry.persist(task)
    return store.read(layout.tasks_directory, task_id)


# --- stream to record ------------------------------------------------------------------------------

def test_claude_cost_and_observed_model_reach_the_record(owned):
    record = persisted(owned, feed(ClaudeBackend(), [init("claude-opus-x"), result(0.25)]))
    assert record.total_cost_usd == 0.25
    assert record.observed_model == {"model": "claude-opus-x", "provenance": "harness_initialization_event"}


def test_mixed_init_models_report_no_model(owned):
    acc = feed(ClaudeBackend(), [init("model-a"), result(0.1), init("model-b"), init("model-a"), result(0.3)])
    assert acc.observed_model == {"model": None, "provenance": "harness_initialization_event_mixed"}
    record = persisted(owned, acc)
    assert record.observed_model == {"model": None, "provenance": "harness_initialization_event_mixed"}
    assert record.total_cost_usd == 0.3


def test_repeated_init_with_the_same_model_stays_observed():
    acc = feed(ClaudeBackend(), [init("model-a"), init("model-a")])
    assert acc.observed_model == {"model": "model-a", "provenance": "harness_initialization_event"}


def test_opencode_cost_is_summed_and_it_observes_no_model(owned):
    events = [{"type": "step_finish", "sessionID": "ses", "part": {"reason": "tool-calls", "type": "step-finish", "cost": 0.5,
                                                                   "tokens": {"input": 1, "output": 1, "cache": {"write": 0, "read": 0}}}}] * 2
    record = persisted(owned, feed(OpencodeBackend(), events), backend="opencode")
    assert record.total_cost_usd == pytest.approx(1.0)
    assert record.observed_model is None


def test_backends_without_cost_leave_it_null(owned):
    record = persisted(owned, Accumulator(), backend="codex")
    assert record.total_cost_usd is None and record.observed_model is None


def test_an_absurdly_large_cost_is_unknown_not_an_error():
    from harflow_mcp import accounting
    assert accounting.cost_value(10**400) is None and accounting.cost_value(-(10**400)) is None
    assert accounting.cost_value(10**6) == 1e6


@pytest.mark.parametrize("bad", [math.nan, math.inf, -0.5, "0.4", True, None])
def test_invalid_cost_becomes_null_without_raising(owned, bad):
    acc = Accumulator()
    acc.total_cost_usd = bad
    record = persisted(owned, acc)
    assert record is not None and record.total_cost_usd is None


@pytest.mark.parametrize("bad", [{"model": 3, "provenance": "p"}, {"model": "m"}, "m", {"model": "m" * 300, "provenance": "p"}])
def test_invalid_observed_model_becomes_null(owned, bad):
    acc = Accumulator()
    acc.observed_model = bad
    assert persisted(owned, acc).observed_model is None


def test_a_zero_cost_is_kept_as_reported(owned):
    assert persisted(owned, feed(ClaudeBackend(), [init("m"), result(0)])).total_cost_usd == 0.0


def test_rewrites_never_erase_a_recorded_cost_or_model(owned):
    first = persisted(owned, feed(ClaudeBackend(), [init("m"), result(0.2)]))
    store.write(owned.tasks_directory, store.replace(first, title="renamed"))
    assert store.read(owned.tasks_directory, "task-1").total_cost_usd == 0.2
    # A writer that observed nothing (a stale rewrite) does not erase either value.
    bare = store.replace(first, total_cost_usd=None, observed_model=None)
    store.write(owned.tasks_directory, bare)
    kept = store.read(owned.tasks_directory, "task-1")
    assert (kept.total_cost_usd, kept.observed_model["model"]) == (0.2, "m")


def test_legacy_record_without_the_fields_still_loads(owned):
    from dataclasses import asdict
    from harflow_mcp import task_documents
    record = persisted(owned, Accumulator())
    document = task_documents.encode(asdict(record))
    document.pop("total_cost_usd"), document.pop("observed_model")
    loaded = task_documents.decode(document, "task-1", store.TaskRecord)
    assert loaded.total_cost_usd is None and loaded.observed_model is None


# --- projection ------------------------------------------------------------------------------------

def source(kind, identity, payload):
    return {"key": f"{kind}:{identity}", "kind": kind, "id": identity, "witness": [1, 2, 3, 4, 5], "payload": payload, "error": None}


def task_payload(task_id, **extra):
    from harflow_mcp import accounting
    return {"task_id": task_id, "backend": "claude", "model": "requested-model", "repo_path": "/repo",
            "started_at": LOWER + 1, "finished_at": LOWER + 5, "status": "completed",
            "accounting": accounting.unavailable(task_id, "fixture_unknown"), **extra}


def executions(sources):
    projection = project(sources, LOWER, UPPER)
    return {item["task_id"]: item for item in projection["items"] if item["kind"] == "execution"}


def test_cost_and_observed_model_facts_never_replace_the_requested_model():
    items = executions([
        source("task", "reported", task_payload("reported", total_cost_usd=0.5, observed_model={"model": "actual-model", "provenance": "harness_initialization_event"})),
        source("task", "silent", task_payload("silent")),
        source("task", "zero", task_payload("zero", total_cost_usd=0.0)),
    ])
    reported, silent, zero = items["reported"], items["silent"], items["zero"]
    assert (reported["cost_usd"], reported["cost_provenance"], reported["cost_additive"]) == (0.5, "backend_reported", True)
    assert (reported["observed_model"], reported["observed_model_provenance"], reported["requested_model"]) == (
        "actual-model", "harness_initialization_event", "requested-model")
    assert (silent["cost_usd"], silent["cost_provenance"], silent["cost_additive"]) == (None, None, False)
    assert silent["observed_model"] is None and silent["observed_model_provenance"] is None
    assert (zero["cost_usd"], zero["cost_additive"]) == (0.0, True)


def test_mixed_model_fact_has_provenance_but_no_model():
    item = executions([source("task", "mixed", task_payload("mixed", observed_model={"model": None, "provenance": "harness_initialization_event_mixed"}))])["mixed"]
    assert item["observed_model"] is None and item["observed_model_provenance"] == "harness_initialization_event_mixed"


@pytest.mark.parametrize("bad", [math.nan, -1.0, "2", True])
def test_invalid_cost_in_a_source_is_unknown(bad):
    item = executions([source("task", "bad", task_payload("bad", total_cost_usd=bad))])["bad"]
    assert item["cost_usd"] is None and item["cost_additive"] is False


def test_a_known_cost_counts_even_when_token_accounting_failed():
    from harflow_mcp import accounting
    payload = task_payload("failed-accounting", total_cost_usd=0.7, accounting=accounting.unavailable("failed-accounting", "accounting_evidence_error"))
    item = executions([source("task", "failed-accounting", payload)])["failed-accounting"]
    assert item["usage_additive"] is False and item["cost_additive"] is True and item["cost_usd"] == 0.7


def workflow_sources(attempt, *, task=None, run_status="completed"):
    run = {"workflow_run_id": "root", "created_at": LOWER + 1, "finished_at": LOWER + 20, "finished_at_sequence": 3,
           "sequence": 3, "status": run_status, "repo_path": "/repo", "name": "root", "workflow_id": "wf", "revision": 1,
           "parent_link": None, "activations": [{"id": "act", "node_id": "node", "role": "node", "tasks": [attempt]}]}
    sources = [source("run", "root", run)]
    if task is not None:
        sources.append(source("task", task["task_id"], task))
    return sources


def attempt(task_id, **extra):
    return {"task_id": task_id, "status": "completed", "started_at": LOWER + 2, "finished_at": LOWER + 9, **extra}


def test_headless_attempt_uses_observed_harness_metadata_when_the_record_has_none():
    from harflow_mcp.statistics_index import _attempt_observed_model
    vibe = _attempt_observed_model({"harness_metadata": {"observed": {"model": "devstral", "active_model": "x"}, "provenance": "harness_session_configuration", "verification_status": "observed_configuration"}})
    assert vibe == {"model": "devstral", "provenance": "harness_session_configuration"}
    configured = _attempt_observed_model({"harness_metadata": {"observed": None, "verification_status": "configured_not_observed", "provenance": "validated_launch_configuration"}})
    assert configured is None
    # Configuration-only evidence is ignored even when it names a model.
    assert _attempt_observed_model({"harness_metadata": {"observed": {"model": "claimed"}, "verification_status": "configured_not_observed", "provenance": "p"}}) is None
    items = executions(workflow_sources(attempt("att", observed_model=vibe), task=task_payload("att")))
    assert items["att"]["observed_model"] == "devstral"
    assert items["att"]["observed_model_provenance"] == "harness_session_configuration"
    # The task's own observation wins over the attempt's.
    own = task_payload("att", observed_model={"model": "own", "provenance": "harness_initialization_event"})
    assert executions(workflow_sources(attempt("att", observed_model=vibe), task=own))["att"]["observed_model"] == "own"


def test_native_rows_take_the_observed_model_only_from_the_attempt():
    child = attempt("child", execution_kind="native_subagent", transport_task_id="t", observed_model={"model": "child-model", "provenance": "observed_native_child"})
    record = task_payload("child", observed_model={"model": "record-model", "provenance": "harness_initialization_event"}, total_cost_usd=3.0)
    row = executions(workflow_sources(child, task=record))["child"]
    assert (row["observed_model"], row["observed_model_provenance"], row["cost_usd"]) == ("child-model", "observed_native_child", None)


def test_native_child_has_no_cost_and_reports_its_observed_model_once():
    from harflow_mcp.statistics_index import _attempt_observed_model
    child = attempt("child", execution_kind="native_subagent", transport_task_id="transport", owner_task_id="transport",
                    harness_metadata={"observed": {"model": "child-model", "native_child_id": "c"}, "verification_status": "observed_native_child"})
    child["observed_model"] = _attempt_observed_model(child)
    assert child["observed_model"] == {"model": "child-model", "provenance": "observed_native_child"}
    from_result = _attempt_observed_model({"execution_kind": "native_subagent", "harness_metadata": {"observed": None}, "result": {"observed_model": "result-model"}})
    assert from_result == {"model": "result-model", "provenance": "observed_native_child"}
    transport = task_payload("transport", total_cost_usd=2.0)
    projection = project(workflow_sources(child, task=transport), LOWER, UPPER)
    rows = {item["task_id"]: item for item in projection["items"] if item["kind"] == "execution"}
    assert rows["child"]["cost_usd"] is None and rows["child"]["cost_additive"] is False
    assert (rows["child"]["observed_model"], rows["child"]["observed_model_provenance"]) == ("child-model", "observed_native_child")
    # The transport's own cost is its row's alone; the child's row adds nothing.
    assert sum(r["cost_usd"] for r in rows.values() if r["cost_additive"]) == 2.0


# --- pages -----------------------------------------------------------------------------------------

def persisted_task(task_id, **extra):
    return store.TaskRecord(task_id=task_id, backend="claude", session_id=None, repo_path="/repo", started_at="2027-01-01T00:00:00+00:00",
                            status="completed", finished_at="2027-01-01T00:00:10+00:00", model=None, **extra)


def page(layout, cursor=None, limit=2):
    return statistics_page(layout, since="2026-12-31T00:00:00+00:00", before="2027-01-02T00:00:00+00:00", limit=limit, cursor=cursor)


def test_cost_coverage_spans_pages_and_counts_known_costs_beside_unknown_ones(owned):
    from harflow_mcp import accounting
    store.write(owned.tasks_directory, persisted_task("a-cost", total_cost_usd=0.4, observed_model={"model": "m", "provenance": "harness_initialization_event"}))
    store.write(owned.tasks_directory, persisted_task("b-nocost"))
    store.write(owned.tasks_directory, persisted_task("c-cost-no-tokens", total_cost_usd=0.1, accounting=accounting.unavailable("c-cost-no-tokens", "accounting_evidence_error")))
    first = page(owned)
    assert first["has_more"] is True
    second = page(owned, cursor=first["next_cursor"])
    for result in (first, second):
        assert result["accounting_coverage"] == {"eligible_executions": 3, "available_executions": 0}
        assert result["cost_coverage"] == {"eligible_executions": 3, "available_executions": 2}
    rows = {item["task_id"]: item for result in (first, second) for item in result["items"] if item["kind"] == "execution"}
    assert rows["a-cost"]["cost_usd"] == 0.4 and rows["a-cost"]["observed_model"] == "m"
    assert rows["b-nocost"]["cost_usd"] is None
    assert rows["c-cost-no-tokens"]["cost_additive"] is True and rows["c-cost-no-tokens"]["usage_additive"] is False


def test_an_older_index_schema_is_rebuilt_not_trusted(owned):
    import sqlite3
    from harflow_mcp.statistics_index import INDEX_FILENAME, SCHEMA_VERSION
    store.write(owned.tasks_directory, persisted_task("t", total_cost_usd=0.9))
    assert page(owned, limit=10)["cost_coverage"]["available_executions"] == 1
    path = owned.root / INDEX_FILENAME
    connection = sqlite3.connect(path)
    connection.execute("UPDATE sources SET payload=json_remove(payload,'$.total_cost_usd')")
    connection.execute(f"PRAGMA user_version={SCHEMA_VERSION - 1}")
    connection.commit()
    connection.close()
    assert SCHEMA_VERSION == 2
    assert page(owned, limit=10)["cost_coverage"] == {"eligible_executions": 1, "available_executions": 1}


def test_recovery_style_rewrite_preserves_the_fields(owned):
    record = store.replace(persisted_task("rec", total_cost_usd=0.3, observed_model={"model": "m", "provenance": "p"}),
                           status="running", finished_at=None)
    store.write(owned.tasks_directory, record)
    interrupted = store.replace(record, status="failed", finished_at="2027-01-01T00:00:20+00:00")
    assert store.write_landed(owned.tasks_directory, interrupted, expected_record=store.read(owned.tasks_directory, "rec"))
    kept = store.read(owned.tasks_directory, "rec")
    assert (kept.status, kept.total_cost_usd, kept.observed_model["model"]) == ("failed", 0.3, "m")
