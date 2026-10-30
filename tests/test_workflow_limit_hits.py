"""Workflow usage-limit evidence, the bounded hit query and its MCP and ctl surfaces."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp import MCPError

from harflow_mcp import catalog, ctl, id_resolution, limit_hits, server, state_ownership, takeover, wire_contracts, workflow_inspection, workflow_limit_hits, workflow_reads, workflow_responses, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.wire_contracts import CTL_FORMAT, validate_envelope
from test_workflow_run_filters import BASE, Clock, _definition, repos  # noqa: F401  (fixtures)

FIXTURES = Path(__file__).parents[1] / "contracts" / "v1"
NOW = 1_800_000_000.0
FUTURE = "2099-01-01T00:00:00Z"
PAST = "2020-01-01T00:00:00+00:00"
RUN_A, RUN_B, RUN_C = "run-aaaaaaaa-0001", "run-bbbbbbbb-0002", "run-cccccccc-0003"


@pytest.fixture
def owned_state(tmp_path):
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


@pytest.fixture
def clock(monkeypatch):
    value = Clock()
    monkeypatch.setattr(workflows, "time", value)
    return value


@pytest.fixture
def storage(owned_state, tmp_path, clock, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda _backend: True)
    return workflows.WorkflowStore(root=owned_state)


@pytest.fixture
def log_dir(storage):
    return storage.root / "tasks"


def diagnostic(**fields):
    return {"category": "usage_limit", "reason": "Usage limit reached", "source": "stderr", **fields}


def attempt(task_id, *, backend="codex", model="gpt-x", finished=BASE + 10, failure=True, observed=None, transport=None, **fields):
    value = {"task_id": task_id, "status": "failed", "candidate": {"backend": backend, "model": model}, "finished_at": finished,
             "result": {"status": "failed", "summary": "failed"}}
    if failure:
        value["result"]["failure_diagnostic"] = diagnostic(reset_at=FUTURE) if failure is True else failure
    if observed is not None:
        value["result"]["observed_model"] = observed
    if transport is not None:
        value["transport_task_id"] = transport
    if finished is None:
        value.pop("finished_at")
    value.update(fields)
    return value


def execution(identifier, node_id, *tasks, role="node"):
    return {"id": identifier, "node_id": node_id, "role": role, "status": "failed", "created_at": BASE, "tasks": list(tasks)}


def link(child, identifier="link"):
    return {"id": identifier + "-" + child[-4:], "node_id": "child", "role": "node", "status": "failed", "created_at": BASE, "tasks": [], "invocation": {"child_workflow_run_id": child}}


def seed(storage, clock, repo, run_id, *executions, workflow_id="wf-a", parent=None, root=None, status="failed", created=BASE, **fields):
    clock.value = created
    run = storage.create_run(_definition("Limit Fixture", workflow_id), "prompt", repo, run_id=run_id)

    def mutate(value):
        value["status"] = status
        value["activations"] = list(executions)
        if parent is not None:
            value["parent_link"] = {"workflow_run_id": parent, "root_workflow_run_id": root or parent, "execution_id": "e1", "node_id": "work", "depth": 1}
        value.update(fields)

    storage.update_run(run["workflow_run_id"], mutate, "fixture")
    return run_id


def rows(storage, table="run_limit_hits"):
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.row_factory = sqlite3.Row
        return [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY 1,2" if table == "run_limit_hits" else f"SELECT * FROM {table} ORDER BY 1")]


def recent(storage, log_dir, managed=None, **filters):
    return workflow_limit_hits.recent(log_dir, managed, filters, now=NOW)


def settle(storage, log_dir, managed=None, **filters):
    """Read until the cold catalog is indexed (a read indexes one bounded batch)."""
    for _ in range(8):
        result = recent(storage, log_dir, managed, **filters)
        if result["index_state"]["status"] != "preparing":
            return result
    raise AssertionError(result)


def hit_ids(result):
    return [hit["task_id"] for hit in result["latest"]]


# ---- evidence extraction ----------------------------------------------------------------------------

def test_hits_come_from_nested_activation_tasks_with_their_identity(storage, clock, repos):
    seed(storage, clock, repos["alpha"], RUN_A,
         execution("e1", "work", attempt("t-codex", model="gpt-x", observed="gpt-x-2026", failure=diagnostic(http_status=429, provider_code="usage_limit_reached", reset_at=FUTURE))),
         execution("e2", "review", attempt("t-ok", failure=False), attempt("t-claude", backend="claude", model="opus", finished=BASE + 20)),
         execution("native", "work", attempt("t-native", execution_kind="native_subagent", transport="t-transport"), role="native_control"))
    hit = {row["hit_key"]: row for row in rows(storage)}
    assert set(hit) == {"t-codex", "t-claude", "t-native"}
    codex = hit["t-codex"]
    assert (codex["root_id"], codex["workflow_id"], codex["backend"], codex["requested_model"], codex["observed_model"], codex["finished_at"]) == (RUN_A, "wf-a", "codex", "gpt-x", "gpt-x-2026", BASE + 10)
    payload = json.loads(codex["payload"])
    assert payload["execution_id"] == "e1" and payload["node_id"] == "work" and payload["role"] == "node" and payload["attempt"] == 1
    assert payload["diagnostic"] == {"category": "usage_limit", "source": "stderr", "reason": "Usage limit reached", "provider_code": "usage_limit_reached", "http_status": 429}
    assert json.loads(hit["t-claude"]["payload"])["attempt"] == 2 and hit["t-claude"]["requested_model"] == "opus"
    assert json.loads(hit["t-native"]["payload"])["transport_task_id"] == "t-transport" and json.loads(hit["t-native"]["payload"])["role"] == "native_control"


@pytest.mark.parametrize("failure", [
    {"category": "provider_error", "reason": "Provider failed", "source": "stderr"},
    {"reason": "You have hit your usage limit", "source": "stderr"},
])
def test_only_a_usage_limit_category_is_evidence(storage, clock, repos, failure):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1", failure=failure)))
    assert rows(storage) == []


def test_node_results_evidence_copies_and_history_are_never_read(storage, clock, repos):
    copied = {"status": "failed", "result": {"failure_diagnostic": diagnostic()}, "evidence": [{"task_id": "t1", "failure_diagnostic": diagnostic()}]}
    seed(storage, clock, repos["alpha"], RUN_A, dict(execution("e1", "work", attempt("t1", failure=False)), node_result=copied),
         usage_recovery_history=[{"decision_id": "d1", "role": "node", "accepted_answer": "fallback", "failure_diagnostic": diagnostic(), "failed_candidate": {"backend": "codex"}}])
    assert rows(storage) == []


def test_each_attempt_is_its_own_hit_including_retries_and_fallbacks(storage, clock, repos):
    seed(storage, clock, repos["alpha"], RUN_A, execution(
        "e1", "work", attempt("first", finished=BASE + 1), attempt("retry", finished=BASE + 2),
        attempt("fallback", backend="claude", model="opus", finished=BASE + 3), attempt("resumed", finished=BASE + 4, resume_task_id="first")))
    assert [row["hit_key"] for row in rows(storage)] == ["fallback", "first", "resumed", "retry"]
    attempts = {row["hit_key"]: json.loads(row["payload"])["attempt"] for row in rows(storage)}
    assert attempts == {"first": 1, "retry": 2, "fallback": 3, "resumed": 4}
    result = settle(storage, storage.root / "tasks")
    assert sorted(hit_ids(result)) == ["fallback", "resumed"]  # latest per (backend, requested, observed) key


def test_children_sharing_a_transport_stay_separate_hits_without_copy_provenance(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution(
        "native", "work",
        attempt("child-1", model="gpt-a", transport="transport-1", finished=BASE + 1, execution_kind="native_subagent"),
        attempt("child-2", model="gpt-b", transport="transport-1", finished=BASE + 2, failure=diagnostic(reason="Other rejection"), execution_kind="native_subagent"),
        attempt("child-3", model="gpt-a", transport="transport-1", finished=BASE + 3, execution_kind="native_subagent"),
        attempt("transport-1", model="gpt-a", finished=BASE + 4), role="native_control"))
    assert {row["hit_key"] for row in rows(storage)} == {"child-1", "child-2", "child-3", "transport-1"}
    items = {hit["task_id"]: hit for hit in settle(storage, log_dir)["latest"]}
    assert items["child-2"]["transport_task_id"] == "transport-1" and items["child-2"]["diagnostic"]["reason"] == "Other rejection"
    assert items["transport-1"]["transport_task_id"] is None
    # Identical rejections on two children are still two attempts; readers group by transport_task_id.
    assert {row["hit_key"] for row in rows(storage) if json.loads(row["payload"])["transport_task_id"] == "transport-1"} == {"child-1", "child-2", "child-3"}


def test_a_hit_without_a_finish_time_falls_back_in_order(storage, clock, repos):
    seed(storage, clock, repos["alpha"], RUN_A, execution(
        "e1", "work", attempt("started", finished=None, started_at=BASE + 3), attempt("none", finished=None)), updated_at=BASE + 50)
    stamps = {row["hit_key"]: (row["finished_at"], json.loads(row["payload"])["observed_at_source"]) for row in rows(storage)}
    assert stamps["started"] == (BASE + 3, "started_at") and stamps["none"][1] == "activation_created_at"


def meta_rows(storage):
    return rows(storage, "run_limit_meta")


def test_a_run_over_the_cap_keeps_the_newest_attempts_and_the_newest_hit_of_each_further_key(storage, clock, repos, log_dir):
    tasks = [attempt(f"old-{number:03}", model="gpt-old", finished=BASE + number) for number in range(5)]
    tasks += [attempt(f"t{number:03}", finished=BASE + 100 + number) for number in range(130)]
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", *tasks))
    keys = {row["hit_key"] for row in rows(storage)}
    assert len(keys) == workflow_limit_hits.RUN_HIT_LIMIT + 1 and "t129" in keys and "t030" in keys and "t029" not in keys and "old-004" in keys and "old-003" not in keys
    (meta,) = meta_rows(storage)
    assert (meta["run_id"], meta["keys_exceeded"]) == (RUN_A, 0)
    # The latest hit per key is intact, so an unfiltered answer is complete; a bound that could hide dropped hits is not.
    assert sorted(hit_ids(settle(storage, log_dir))) == ["old-004", "t129"] and settle(storage, log_dir)["scan"]["complete"] is True
    bounded = settle(storage, log_dir, before=limit_hits._iso(BASE + 50))
    assert hit_ids(bounded) == ["old-004"] and bounded["scan"]["complete"] is False
    assert settle(storage, log_dir, workflow_id="wf-none")["scan"]["complete"] is True


def test_a_run_with_more_keys_than_the_key_limit_is_incomplete_even_unfiltered(storage, clock, repos, log_dir):
    tasks = [attempt(f"k{number:04}", model=f"model-{number:04}", finished=BASE + number) for number in range(400)]
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", *tasks))
    assert len(rows(storage)) == workflow_limit_hits.MAX_RUN_ROWS
    (meta,) = meta_rows(storage)
    assert meta["keys_exceeded"] == 1
    result = settle(storage, log_dir)
    assert result["scan"]["complete"] is False and len(result["latest"]) == workflow_limit_hits.MAX_KEYS
    assert settle(storage, log_dir, workflow_id="wf-none")["scan"]["complete"] is True


def test_an_untruncated_run_has_no_meta_row_and_is_complete(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1")))
    assert meta_rows(storage) == [] and settle(storage, log_dir, before="2099-01-01T00:00:00Z")["scan"]["complete"] is True


def test_retention_work_per_run_is_flat_in_the_number_of_distinct_keys(storage, clock, repos):
    def managed_steps(count, run_id):
        tasks = [attempt(f"{run_id[-4:]}-{number:05}", model=f"m{number:05}", finished=BASE + number) for number in range(count)]
        seed(storage, clock, repos["alpha"], run_id, execution("e1", "work", *tasks))
        return window(storage, {}, run_ids=[run_id])
    (small, _, _), small_steps = managed_steps(200, "run-small-00000001")
    (large, _, _), large_steps = managed_steps(3000, "run-large-00000002")
    assert len(small) == len(large) == workflow_limit_hits.MAX_RUN_ROWS
    assert large_steps <= small_steps * 1.2


def test_accepted_recovery_annotates_only_the_matching_hit(storage, clock, repos, log_dir):
    history = [{"decision_id": "d1", "role": "node", "accepted_answer": "fallback", "accepted_at": BASE + 2, "failure_diagnostic": diagnostic(reset_at=FUTURE),
                "failed_candidate": {"backend": "codex", "model": "gpt-x"}}]
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("limited", finished=BASE + 1), attempt("other", backend="claude", model="opus", finished=BASE + 2)),
         usage_recovery_history=history)
    items = {hit["task_id"]: hit for hit in settle(storage, log_dir)["latest"]}
    assert items["limited"]["recovery"] == {"accepted": True, "answer": "fallback"} and "recovery" not in items["other"]


def test_a_repeated_limit_after_a_resume_is_not_marked_recovered(storage, clock, repos, log_dir):
    history = [{"decision_id": "d1", "role": "node", "accepted_answer": "retry_current", "accepted_at": BASE + 150, "failure_diagnostic": diagnostic(),
                "failed_candidate": {"backend": "codex", "model": "gpt-x"}}]
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("before", finished=BASE + 100), attempt("after", finished=BASE + 200)), usage_recovery_history=history)
    items = {hit["task_id"]: hit for hit in settle(storage, log_dir)["latest"]}
    assert "after" in items and "recovery" not in items["after"]
    only = {row["hit_key"]: json.loads(row["payload"]) for row in rows(storage)}
    assert only["before"]["recovery"] == {"accepted": True, "answer": "retry_current"} and "recovery" not in only["after"]


def test_two_answers_annotate_two_successive_identical_hits_in_order(storage, clock, repos):
    entry = lambda at, answer: {"decision_id": answer, "role": "node", "accepted_answer": answer, "accepted_at": at, "failure_diagnostic": diagnostic(), "failed_candidate": {"backend": "codex", "model": "gpt-x"}}  # noqa: E731
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("h1", finished=BASE + 10), attempt("h2", finished=BASE + 30), attempt("h3", finished=BASE + 50)),
         usage_recovery_history=[entry(BASE + 20, "first"), entry(BASE + 40, "second")])
    got = {row["hit_key"]: json.loads(row["payload"]).get("recovery", {}).get("answer") for row in rows(storage)}
    assert got == {"h1": "first", "h2": "second", "h3": None}


def test_tied_candidates_are_left_unannotated(storage, clock, repos):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("h1", finished=BASE + 10), attempt("h2", finished=BASE + 10)),
         usage_recovery_history=[{"decision_id": "d", "role": "node", "accepted_answer": "fallback", "accepted_at": BASE + 20, "failure_diagnostic": diagnostic(), "failed_candidate": {"backend": "codex", "model": "gpt-x"}}])
    assert all("recovery" not in json.loads(row["payload"]) for row in rows(storage))


# ---- blocking state ---------------------------------------------------------------------------------

def worker_recovery(**fields):
    return {"node_id": "work", "execution_id": "e1", "decision_id": "d-worker", "failure_diagnostic": diagnostic(reset_at=FUTURE), **fields}


def test_blocking_is_reported_on_the_owning_child_not_the_running_root(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work"), status="running", created=BASE)
    seed(storage, clock, repos["alpha"], RUN_B, execution("e1", "work", attempt("child-limit", model="gpt-c")), status="needs_input", parent=RUN_A,
         worker_usage_recovery=worker_recovery(), created=BASE + 1)
    assert [row["run_id"] for row in rows(storage, "run_limit_blocking")] == [RUN_B]
    result = settle(storage, log_dir)
    (current,) = result["current"]
    assert current["workflow_run_id"] == RUN_B and current["root_workflow_run_id"] == RUN_A and current["role"] == "worker"
    assert (current["backend"], current["requested_model"], current["node_id"], current["execution_id"], current["decision_id"]) == ("codex", "gpt-c", "work", "e1", "d-worker")
    assert current["run_status"] == "needs_input" and current["reset"]["state"] == "future"
    assert current["detail_ref"] == {"tool": "get_workflow_run_detail", "workflow_run_id": RUN_B, "view": "execution:e1"}


@pytest.mark.parametrize("status", ["cancelled", "cancelling", "completed", "failed"])
def test_a_finished_or_cancelled_run_no_longer_blocks(storage, clock, repos, log_dir, status):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1")), worker_usage_recovery=worker_recovery(), status="needs_input")
    assert settle(storage, log_dir)["current"]
    storage.update_run(RUN_A, lambda run: run.update(status=status), "cancel")
    result = settle(storage, log_dir)
    assert result["current"] == [] and rows(storage, "run_limit_blocking") == [] and hit_ids(result) == ["t1"]


def test_orchestrator_recovery_blocks_a_running_run(storage, clock, repos, log_dir):
    recovery = {"decision_id": "d-orch", "role": "orchestrator", "failure_diagnostic": diagnostic(), "failed_candidate": {"backend": "claude", "model": "opus"}, "choices": []}
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work"), status="running", orchestrator_usage_recovery=recovery)
    (current,) = settle(storage, log_dir)["current"]
    assert (current["workflow_run_id"], current["role"], current["backend"], current["requested_model"], current["run_status"]) == (RUN_A, "orchestrator", "claude", "opus", "running")
    assert current["execution_id"] is None and "detail_ref" not in current and current["reset"] == {"state": "unknown"}


def test_a_recovered_run_stops_blocking(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1")), worker_usage_recovery=worker_recovery(), status="needs_input")
    assert settle(storage, log_dir)["current"]
    storage.update_run(RUN_A, lambda run: (run.pop("worker_usage_recovery"), run.update(status="running")), "answered")
    result = settle(storage, log_dir)
    assert result["current"] == [] and rows(storage, "run_limit_blocking") == [] and hit_ids(result) == ["t1"]


# ---- catalog lifecycle ------------------------------------------------------------------------------

def test_rewrite_replaces_all_rows_of_the_run_atomically(storage, clock, repos):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1"), attempt("a2")), worker_usage_recovery=worker_recovery(), status="needs_input")
    assert {row["hit_key"] for row in rows(storage)} == {"a1", "a2"}
    storage.update_run(RUN_A, lambda run: run["activations"][0].update(tasks=[attempt("a2", model="gpt-new"), attempt("a3")]), "rewrite")
    after = {row["hit_key"]: row["requested_model"] for row in rows(storage)}
    assert after == {"a2": "gpt-new", "a3": "gpt-x"}
    storage.update_run(RUN_A, lambda run: run["activations"][0].update(tasks=[attempt("a2", failure=False)]), "cleared")
    assert rows(storage) == [] and [row["run_id"] for row in rows(storage, "run_limit_blocking")] == [RUN_A]


def test_a_failed_projection_leaves_the_previous_rows(storage, clock, repos, monkeypatch):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1")), worker_usage_recovery=worker_recovery(), status="needs_input")
    before = rows(storage)
    good = workflow_limit_hits.evidence

    def broken(run, meta):
        hits, blocking, truncation = good(run, meta)
        return [dict(hit, finished_at=None) for hit in hits], blocking, truncation

    monkeypatch.setattr(workflow_limit_hits, "evidence", broken)
    index, _ = storage.limit_hit_catalog()
    run = storage.get_run(RUN_A)
    with state_ownership.state_session(storage.state_layout), index.connect() as db, pytest.raises(sqlite3.IntegrityError):
        index._put(db, storage._catalog_run_header(run), float(run["created_at"]), False, RUN_A)
    monkeypatch.undo()
    assert rows(storage) == before and len(rows(storage, "run_limit_blocking")) == 1


def test_remove_and_discovery_delete_rows_of_gone_runs(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1")))
    seed(storage, clock, repos["alpha"], RUN_B, execution("e1", "work", attempt("b1", model="gpt-b")), worker_usage_recovery=worker_recovery(), status="needs_input", created=BASE + 1)
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout):
        index.remove(RUN_A)
    assert {row["run_id"] for row in rows(storage)} == {RUN_B}
    (storage.runs / f"{RUN_B}.json").unlink()
    (storage.runs / f"{RUN_A}.json").unlink()
    result = settle(storage, log_dir)
    assert rows(storage) == [] and rows(storage, "run_limit_blocking") == [] and result["latest"] == [] and result["current"] == []


def test_discovery_prunes_orphaned_rows_of_both_tables(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1")))
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.execute("INSERT INTO run_limit_hits VALUES ('ghost','g1','ghost',NULL,'/r','codex','m',NULL,1.0,'{}')")
        db.execute("INSERT INTO run_limit_blocking VALUES ('ghost','ghost',NULL,'/r','{}')")
        db.execute("DELETE FROM state WHERE key IN ('complete','directory_mtime')")
    settle(storage, log_dir)
    assert {row["run_id"] for row in rows(storage)} == {RUN_A} and rows(storage, "run_limit_blocking") == []


def test_existing_runs_are_reprojected_after_the_version_bump(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1")), worker_usage_recovery=worker_recovery(), status="needs_input")
    assert catalog.RUN_PROJECTION_VERSION == "4"
    path = catalog.catalog_path(storage.runs)
    assert path.name == ".listing.v11.sqlite3"
    # Reproduce the previous engine: same file, projection 3, no side-table rows.
    with sqlite3.connect(path) as db:
        db.execute("DELETE FROM run_limit_hits")
        db.execute("DELETE FROM run_limit_blocking")
        db.execute("INSERT OR REPLACE INTO state VALUES ('run_projection_version','3')")
        assert db.execute("SELECT COUNT(*) FROM entries").fetchone() == (1,)
    result = settle(storage, log_dir)
    assert hit_ids(result) == ["a1"] and [item["workflow_run_id"] for item in result["current"]] == [RUN_A]
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT value FROM state WHERE key='run_projection_version'").fetchone() == ("4",)
        assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ("11",)


def test_a_catalog_that_predates_the_tables_gains_them(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1")))
    with sqlite3.connect(catalog.catalog_path(storage.runs)) as db:
        db.execute("DROP TABLE run_limit_hits")
        db.execute("DROP TABLE run_limit_blocking")
        db.execute("INSERT OR REPLACE INTO state VALUES ('run_projection_version','3')")
    assert hit_ids(settle(storage, log_dir)) == ["a1"]


# ---- unavailable runs -------------------------------------------------------------------------------

def test_unreadable_runs_are_listed_as_unavailable_and_cap_at_twenty(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1")))
    for number in range(25):
        (storage.runs / f"broken-{number:03}.json").write_text("{")
    result = settle(storage, log_dir)
    assert hit_ids(result) == ["a1"] and result["scan"]["complete"] is False
    assert [item["workflow_run_id"] for item in result["unavailable"]] == [f"broken-{number:03}" for number in range(20)]
    assert set(result["unavailable"][0]) == {"workflow_run_id", "reason", "code"} and result["unavailable"][0]["code"] == "workflow_run_incompatible"
    for number in range(5):
        (storage.runs / f"broken-{number + 20:03}.json").unlink()
    for number in range(20):
        (storage.runs / f"broken-{number:03}.json").unlink()
    (storage.runs / "broken-000.json").write_text("{")
    result = settle(storage, log_dir)
    assert [item["workflow_run_id"] for item in result["unavailable"]] == ["broken-000"] and result["scan"]["complete"] is False
    (storage.runs / "broken-000.json").unlink()
    result = settle(storage, log_dir)
    assert result["unavailable"] == [] and result["scan"]["complete"] is True


# ---- query ------------------------------------------------------------------------------------------

def test_latest_hit_per_backend_and_models_with_item_shape(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution(
        "e1", "work", attempt("old", finished=BASE + 1), attempt("new", finished=BASE + 9, failure=diagnostic(reset_at=PAST)),
        attempt("other-model", model="gpt-y", finished=BASE + 5, observed="gpt-y-obs"), attempt("claude", backend="claude", model=None, finished=BASE + 6)))
    result = settle(storage, log_dir)
    assert [(hit["backend"], hit["requested_model"], hit["observed_model"], hit["task_id"]) for hit in result["latest"]] == [
        ("codex", "gpt-x", None, "new"), ("claude", None, None, "claude"), ("codex", "gpt-y", "gpt-y-obs", "other-model")]
    assert result["scan"] == {"examined": 4, "limit": 500, "current_examined": 0, "current_limit": 100, "complete": True}
    assert result["index_state"]["status"] == "ready" and result["unavailable"] == []
    assert result["latest"][0] == {
        "root_workflow_run_id": RUN_A, "workflow_run_id": RUN_A, "node_id": "work", "role": "node", "execution_id": "e1", "attempt": 2,
        "task_id": "new", "transport_task_id": None, "backend": "codex", "requested_model": "gpt-x", "observed_model": None,
        "observed_at": limit_hits._iso(BASE + 9), "observed_at_source": "finished_at",
        "diagnostic": {"category": "usage_limit", "source": "stderr", "reason": "Usage limit reached"},
        "reset": {"raw": PAST, "at": PAST, "state": "elapsed"},
        "detail_ref": {"tool": "get_workflow_run_detail", "workflow_run_id": RUN_A, "view": "execution:e1"}}


def test_detail_ref_reads_the_execution_through_the_detail_view(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1")))
    (hit,) = settle(storage, log_dir)["latest"]
    ref = hit["detail_ref"]
    page = workflow_responses.detail(storage.get_run(ref["workflow_run_id"]), ref["view"], None, 8000)
    assert json.loads(page["chunk"])["id"] == "e1"


@pytest.mark.parametrize("reset, expected", [
    (FUTURE, "future"), (PAST, "elapsed"), (None, "unknown"), (1_600_000_000, "elapsed"),
])
def test_reset_states(storage, clock, repos, log_dir, reset, expected):
    failure = diagnostic(**({} if reset is None else {"reset_at": reset}))
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1", failure=failure)))
    (hit,) = settle(storage, log_dir)["latest"]
    assert hit["reset"]["state"] == expected and ("raw" in hit["reset"]) == (expected != "unknown")


def test_a_malformed_reset_makes_the_run_unavailable_rather_than_unknown(storage, clock, repos, log_dir):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("t1")))
    path = storage.runs / f"{RUN_A}.json"
    path.write_text(path.read_text().replace(FUTURE, "next tuesday"))
    result = settle(storage, log_dir)
    assert result["latest"] == [] and [item["workflow_run_id"] for item in result["unavailable"]] == [RUN_A] and result["scan"]["complete"] is False


def two_workflows(storage, clock, repos):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a1", finished=BASE + 10)), workflow_id="wf-a", created=BASE)
    seed(storage, clock, repos["beta"], RUN_B, execution("e1", "work", attempt("b1", backend="claude", model="opus", finished=BASE + 20)), workflow_id="wf-b", created=BASE + 1)
    seed(storage, clock, repos["beta"], RUN_C, execution("e1", "work", attempt("c1", model="gpt-c", finished=BASE + 30)), workflow_id="wf-b", parent=RUN_B, created=BASE + 2,
         worker_usage_recovery=worker_recovery(), status="needs_input")


def test_each_filter(storage, clock, repos, log_dir):
    two_workflows(storage, clock, repos)
    beta = str(repos["beta"].resolve())
    iso = lambda value: limit_hits._iso(value)  # noqa: E731
    def ids(**filters):
        result = settle(storage, log_dir, **filters)
        return sorted(hit_ids(result)), [item["workflow_run_id"] for item in result["current"]]
    assert ids() == (["a1", "b1", "c1"], [RUN_C])
    assert ids(workflow_id="wf-a") == (["a1"], [])
    assert ids(workflow_id="wf-b") == (["b1", "c1"], [RUN_C])
    assert ids(repo=beta) == (["b1", "c1"], [RUN_C])
    assert ids(root_workflow_run_id=RUN_B) == (["b1", "c1"], [RUN_C])
    assert ids(root_workflow_run_id=RUN_A) == (["a1"], [])
    assert ids(since=iso(BASE + 20)) == (["b1", "c1"], [RUN_C])
    assert ids(before=iso(BASE + 20)) == (["a1"], [RUN_C])  # before and since bound hits, never blocking rows
    assert ids(since=iso(BASE + 11), before=iso(BASE + 30)) == (["b1"], [RUN_C])
    assert ids(workflow_id="wf-b", repo=str(repos["alpha"].resolve())) == ([], [])
    assert ids(root_workflow_run_id=RUN_B, workflow_id="wf-b", repo=beta, since=iso(BASE)) == (["b1", "c1"], [RUN_C])
    only = settle(storage, log_dir, current_only=True)
    assert only["latest"] == [] and [item["workflow_run_id"] for item in only["current"]] == [RUN_C] and only["scan"]["examined"] == 0


@pytest.mark.parametrize("filters, message", [
    ({"nope": "x"}, "workflow limit hit filters"),
    ({"since": "yesterday"}, "since"),
    ({"before": "2026-01-01T00:00:00"}, "timezone"),
    ({"workflow_id": " "}, "workflow_id"),
    ({"repo": "x" * 5000}, "repo"),
    ({"root_workflow_run_id": ""}, "root_workflow_run_id"),
    ({"current_only": "yes"}, "current_only"),
])
def test_invalid_filters_are_refused_before_any_lookup(storage, log_dir, monkeypatch, filters, message):
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda *a, **k: pytest.fail("authority work before validation"))
    import asyncio
    with pytest.raises(ValueError, match=message):
        asyncio.run(workflow_reads.call("limit_hits", directory=log_dir, filters=filters))


def test_key_cap_reports_incomplete(storage, clock, repos, log_dir):
    tasks = [attempt(f"t{number:03}", model=f"model-{number:03}", finished=BASE + number) for number in range(55)]
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", *tasks))
    result = settle(storage, log_dir)
    assert len(result["latest"]) == 50 and result["scan"]["complete"] is False and result["scan"]["examined"] == 55


# ---- bounded work -----------------------------------------------------------------------------------

def row(number, stamp, *, workflow="wf-x", repo="/r", root="root-x", run="run-x"):
    return (run, f"hit{number:05}", root, workflow, repo, "codex", "m", None, stamp, json.dumps({"task_id": f"hit{number:05}"}))


def insert_rows(storage, rows_, blocking=()):
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.executemany("INSERT INTO run_limit_hits VALUES (?,?,?,?,?,?,?,?,?,?)", rows_)
        db.executemany("INSERT INTO run_limit_blocking VALUES (?,?,?,?,?)", blocking)


def window(storage, filters, limit=500, run_ids=None):
    index, _ = storage.limit_hit_catalog()
    steps = [0]
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.set_progress_handler(lambda: steps.__setitem__(0, steps[0] + 1) or 0, 1)
        result = index.run_limit_hit_window(db, limit, filters, run_ids)
        db.set_progress_handler(None, 0)
    return result, steps[0]


FILTER_PATHS = {
    "chronology": {},
    "since": {"since": 0.0},
    "workflow": {"workflow_id": "wf-x"},
    "repo": {"repo": "/r"},
    "root": {"root_workflow_run_id": "root-x"},
    "root+workflow+repo": {"root_workflow_run_id": "root-x", "workflow_id": "wf-x", "repo": "/r"},
    "workflow+range": {"workflow_id": "wf-x", "since": 0.0, "before": 4e9},
}


def test_tied_timestamps_keep_the_window_and_work_bounded(storage):
    insert_rows(storage, [row(n, 1_790_000_000.0) for n in range(600)])
    small = {name: window(storage, filters) for name, filters in FILTER_PATHS.items()}
    insert_rows(storage, [row(n, 1_790_000_000.0) for n in range(600, 6000)])
    for name, filters in FILTER_PATHS.items():
        (hits, examined, capped), steps = window(storage, filters)
        assert (len(hits), examined, capped) == (500, 500, True), name
        # Ten times the rows, all tied, does not widen the examined window.
        assert steps <= small[name][1] * 1.2, name
    (hits, _, _), _ = window(storage, {"workflow_id": "wf-x"})
    assert [payload["task_id"] for *_, payload in hits][:2] == ["hit05999", "hit05998"]


def test_since_and_before_are_applied_before_the_cap(storage):
    insert_rows(storage, [row(n, 1_790_000_000.0 + n) for n in range(700)])
    (hits, examined, capped), _ = window(storage, {"since": 1_790_000_300.0})
    assert (len(hits), examined, capped) == (400, 400, False)
    (hits, examined, capped), _ = window(storage, {"since": 1_790_000_100.0})
    assert (len(hits), examined, capped) == (500, 500, True)
    (hits, examined, capped), _ = window(storage, {"before": 1_790_000_100.0})
    assert (len(hits), examined, capped) == (100, 100, False)


def traced_statements(storage, call):
    index, _ = storage.limit_hit_catalog()
    statements = []
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.set_trace_callback(statements.append)
        call(index, db)
        db.set_trace_callback(None)
        plans = {}
        for statement in statements:
            if statement.lstrip().startswith("SELECT") and "run_limit_" in statement:
                plans[statement] = [detail for *_, detail in db.execute("EXPLAIN QUERY PLAN " + statement)]
    return plans


@pytest.mark.parametrize("name, index_name", [
    ("chronology", "run_limit_hit_chronology"), ("since", "run_limit_hit_chronology"),
    ("workflow", "run_limit_hit_workflow"), ("repo", "run_limit_hit_repo"), ("root", "run_limit_hit_root"),
    ("root+workflow+repo", "run_limit_hit_root"), ("workflow+range", "run_limit_hit_workflow"),
])
def test_hit_query_plan_walks_an_ordered_index_inside_the_window(storage, name, index_name):
    insert_rows(storage, [row(n, 1_790_000_000.0) for n in range(50)])
    plans = traced_statements(storage, lambda index, db: index.run_limit_hit_window(db, 500, FILTER_PATHS[name]))
    assert plans
    for statement, plan in plans.items():
        assert any(f"USING COVERING INDEX {index_name}" in detail for detail in plan), plan
        if "JOIN" in statement:
            assert "SEARCH t USING INDEX sqlite_autoindex_run_limit_hits_1 (run_id=? AND hit_key=?)" in plan
            # The inner window is ordered by the index: the only sort is the outer one over the window.
            assert plan.count("USE TEMP B-TREE FOR ORDER BY") == 1, plan
        else:
            assert "USE TEMP B-TREE FOR ORDER BY" not in plan, plan


def test_blocking_query_plan_walks_an_ordered_index(storage):
    insert_rows(storage, [], [(f"run{n:03}", "root-x", "wf-x", "/r", "{}") for n in range(30)])
    for name, filters, index_name in (("unfiltered", {}, "sqlite_autoindex_run_limit_blocking_1"), ("workflow", {"workflow_id": "wf-x"}, "run_limit_blocking_workflow"),
                                      ("repo", {"repo": "/r"}, "run_limit_blocking_repo"), ("root", {"root_workflow_run_id": "root-x"}, "run_limit_blocking_root")):
        plans = traced_statements(storage, lambda index, db: index.run_limit_blocking_window(db, 100, filters))
        assert any("JOIN" in statement and any(index_name in detail and "INDEX" in detail for detail in plan) and plan.count("USE TEMP B-TREE FOR ORDER BY") <= 1 for statement, plan in plans.items()), (name, plans)


def test_managed_query_plan_is_bounded_by_the_member_set(storage):
    insert_rows(storage, [row(n, 1_790_000_000.0 + n, run=f"run{n % 3}") for n in range(60)])
    members = ["run0", "run1"]
    plans = traced_statements(storage, lambda index, db: index.run_limit_hit_window(db, 500, {"workflow_id": "wf-x"}, members))
    assert plans
    for statement, plan in plans.items():
        assert any("USING INDEX run_limit_hit_run (run_id=?)" in detail for detail in plan), plan
    (hits, examined, capped), _ = window(storage, {"workflow_id": "wf-x"}, run_ids=members)
    assert (len(hits), examined, capped) == (40, 40, False) and {hit[1] for hit in hits} == set(members)


def truncation_steps(storage, filters, run_ids=None, any_truncation=True):
    index, _ = storage.limit_hit_catalog()
    steps = [0]
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.set_progress_handler(lambda: steps.__setitem__(0, steps[0] + 1) or 0, 1)
        found = index.run_limit_truncated(db, filters, run_ids, any_truncation=any_truncation)
        db.set_progress_handler(None, 0)
    return found, steps[0]


TRUNCATION_PATHS = {
    "root": ({"root_workflow_run_id": "absent"}, None), "workflow": ({"workflow_id": "absent"}, None), "repo": ({"repo": "/absent"}, None),
    "everything-any": ({}, None), "everything-keys": ({}, None), "managed": ({}, ["absent-run"]),
}


def insert_meta(storage, count, offset=0):
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.executemany("INSERT INTO run_limit_meta VALUES (?,?,?,?,?)", [(f"trunc{n:06}", f"root{n % 50}", f"wf{n % 50}", f"/r{n % 50}", 0) for n in range(offset, offset + count)])


def test_truncation_lookup_work_does_not_grow_with_truncated_runs(storage):
    insert_meta(storage, 50)
    small = {name: truncation_steps(storage, filters, ids, any_truncation=name != "everything-keys") for name, (filters, ids) in TRUNCATION_PATHS.items()}
    insert_meta(storage, 5000, 50)
    for name, (filters, ids) in TRUNCATION_PATHS.items():
        found, steps = truncation_steps(storage, filters, ids, any_truncation=name != "everything-keys")
        # A workflow, repo or root absent from history stays a constant-cost miss however many other runs are truncated.
        assert found is (name == "everything-any"), name
        assert steps <= small[name][1] * 1.2 + 5, (name, steps, small[name][1])


@pytest.mark.parametrize("name, index_name, fragment", [
    ("root", "run_limit_meta_root", "root_id=?"), ("workflow", "run_limit_meta_workflow", "workflow_id=?"), ("repo", "run_limit_meta_repo", "repo_path=?"),
    ("everything-keys", "run_limit_meta_keys", "keys_exceeded=?"), ("managed", "sqlite_autoindex_run_limit_meta_1", "run_id=?"),
])
def test_truncation_query_plan_uses_an_index_and_stops_at_one_row(storage, name, index_name, fragment):
    insert_meta(storage, 20)
    filters, ids = TRUNCATION_PATHS[name]
    plans = traced_statements(storage, lambda index, db: index.run_limit_truncated(db, filters, ids, any_truncation=False))
    assert plans
    for statement, plan in plans.items():
        assert "LIMIT 1" in statement and any(index_name in detail and fragment in detail for detail in plan), plan


def storm(storage, clock, repos, count_runs=6, per_run=100):
    """600 newer hits of one workflow, repo and root, as six runs of 100 attempts."""
    for number in range(count_runs):
        tasks = [attempt(f"s{number}-{index:03}", finished=BASE + 1000 + number * 100 + index) for index in range(per_run)]
        seed(storage, clock, repos["alpha"], f"storm-{number:08}", execution("e1", "work", *tasks), workflow_id="wf-storm", created=BASE + 100 + number)


def test_unfiltered_query_sees_only_the_newest_500_hits(storage, clock, repos, log_dir):
    storm(storage, clock, repos)
    seed(storage, clock, repos["beta"], RUN_B, execution("e1", "work", attempt("rare", backend="claude", model="opus", finished=BASE + 5)), workflow_id="wf-rare", created=BASE)
    result = settle(storage, log_dir)
    assert {hit["backend"] for hit in result["latest"]} == {"codex"}
    assert result["scan"]["examined"] == 500 and result["scan"]["complete"] is False


@pytest.mark.parametrize("filters", [
    {"workflow_id": "wf-rare"}, {"root_workflow_run_id": RUN_B}, {"repo": "beta"}, {"workflow_id": "wf-rare", "since": "1970-01-01T00:00:00Z"},
])
def test_a_rare_filter_value_is_reachable_behind_a_storm(storage, clock, repos, log_dir, filters):
    storm(storage, clock, repos)
    seed(storage, clock, repos["beta"], RUN_B, execution("e1", "work", attempt("rare", backend="claude", model="opus", finished=BASE + 5)), workflow_id="wf-rare", created=BASE)
    filters = {key: str(repos["beta"].resolve()) if value == "beta" else value for key, value in filters.items()}
    result = settle(storage, log_dir, **filters)
    assert hit_ids(result) == ["rare"] and result["scan"] == {"examined": 1, "limit": 500, "current_examined": 0, "current_limit": 100, "complete": True}


def test_the_bound_window_is_still_capped_and_other_filters_apply_inside_it(storage, clock, repos, log_dir):
    storm(storage, clock, repos)
    capped = settle(storage, log_dir, workflow_id="wf-storm")
    assert capped["scan"]["examined"] == 500 and capped["scan"]["complete"] is False
    # repo filters inside the workflow window: the alpha repo holds every storm hit.
    inside = settle(storage, log_dir, workflow_id="wf-storm", repo=str(repos["alpha"].resolve()))
    assert inside["latest"] and inside["scan"]["examined"] == 500
    assert settle(storage, log_dir, workflow_id="wf-storm", repo=str(repos["beta"].resolve()))["latest"] == []


def test_blocking_runs_are_capped_at_one_hundred(storage, log_dir):
    insert_rows(storage, [], [(f"run{n:04}", "root-x", "wf-x", "/r", json.dumps({"run_status": "needs_input", "diagnostic": {}})) for n in range(130)])
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        found, capped = index.run_limit_blocking_window(db, 100, {})
        assert (len(found), capped) == (100, True) and found[0][0] == "run0000"
        found, capped = index.run_limit_blocking_window(db, 100, {"workflow_id": "wf-x", "repo": "/r"})
        assert (len(found), capped) == (100, True)
        assert index.run_limit_blocking_window(db, 100, {"workflow_id": "wf-none"}) == ([], False)


# ---- managed orchestrator scoping -------------------------------------------------------------------

def association(role="orchestrator"):
    return {"role": role, "workflow_run_id": RUN_A, "activation_id": "decision"}


def tree(storage, clock, repos):
    """RUN_A owns RUN_B; RUN_C is an unrelated sibling root; RUN_B's own child is d."""
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a-hit")), link(RUN_B), created=BASE)
    seed(storage, clock, repos["alpha"], RUN_B, execution("e1", "work", attempt("b-hit", model="gpt-b")), link("run-dddddddd-0004"), parent=RUN_A, created=BASE + 1)
    seed(storage, clock, repos["alpha"], "run-dddddddd-0004", execution("e1", "work", attempt("d-hit", model="gpt-d")), parent=RUN_B, root=RUN_A, created=BASE + 2,
         worker_usage_recovery=worker_recovery(), status="needs_input")
    seed(storage, clock, repos["alpha"], RUN_C, execution("e1", "work", attempt("c-hit", model="gpt-c")), created=BASE + 3,
         worker_usage_recovery=worker_recovery(), status="needs_input")


@pytest.fixture
def verified(monkeypatch):
    """Edge verification is exercised separately (`_edge_verified`); here the persisted links are assumed good."""
    calls = []
    monkeypatch.setattr(workflow_limit_hits, "_edge_verified", lambda parent, activation, child, root: calls.append(child["workflow_run_id"]) or True)
    return calls


def managed_for(storage, run_id):
    return association(), storage.get_run(run_id)


def test_orchestrator_sees_its_own_run_and_verified_descendants_only(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    result = settle(storage, log_dir, managed_for(storage, RUN_A))
    assert sorted(hit_ids(result)) == ["a-hit", "b-hit", "d-hit"] and [item["workflow_run_id"] for item in result["current"]] == ["run-dddddddd-0004"]
    assert "c-hit" not in json.dumps(result) and RUN_C not in json.dumps(result)
    assert result["scan"]["complete"] is True and result["unavailable"] == []
    assert result["index_state"] == {"status": "ready", "source": "workflow_catalog", "pending_records": 0, "blocked_records": 0}


def test_a_nested_orchestrator_never_sees_ancestors_or_siblings(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    result = settle(storage, log_dir, managed_for(storage, RUN_B))
    assert sorted(hit_ids(result)) == ["b-hit", "d-hit"]
    assert "a-hit" not in json.dumps(result) and RUN_A not in {hit["workflow_run_id"] for hit in result["latest"]}
    leaf = settle(storage, log_dir, managed_for(storage, "run-dddddddd-0004"))
    assert hit_ids(leaf) == ["d-hit"]


def test_managed_filters_still_apply(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    managed = managed_for(storage, RUN_A)
    assert sorted(hit_ids(settle(storage, log_dir, managed, since=limit_hits._iso(BASE + 11)))) == []
    assert settle(storage, log_dir, managed, current_only=True)["latest"] == []
    assert sorted(hit_ids(settle(storage, log_dir, managed, workflow_id="wf-a"))) == ["a-hit", "b-hit", "d-hit"]


def test_managed_callers_may_not_name_a_tree_root(storage, clock, repos, log_dir, verified, monkeypatch):
    tree(storage, clock, repos)
    callers = [managed_for(storage, run) for run in (RUN_A, RUN_B)]
    monkeypatch.setattr(catalog.Catalog, "connect", lambda *a, **k: pytest.fail("catalog opened before refusal"))
    for caller in callers:
        with pytest.raises(workflow_limit_hits.Refused, match="not supported for managed callers"):
            workflow_limit_hits.recent(log_dir, caller, {"root_workflow_run_id": RUN_A})


def test_a_corrupt_foreign_run_does_not_affect_a_managed_read(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    (storage.runs / "foreign-broken.json").write_text("{")
    direct = settle(storage, log_dir)
    assert [item["workflow_run_id"] for item in direct["unavailable"]] == ["foreign-broken"]
    result = settle(storage, log_dir, managed_for(storage, RUN_A))
    assert sorted(hit_ids(result)) == ["a-hit", "b-hit", "d-hit"] and result["unavailable"] == [] and result["scan"]["complete"] is True
    assert "foreign-broken" not in json.dumps(result) and result["current"]


def test_a_pending_foreign_run_does_not_make_a_managed_read_preparing(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    seed(storage, clock, repos["beta"], "late-foreign-run", execution("e1", "work", attempt("late")), created=BASE + 9)
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.execute("INSERT OR IGNORE INTO pending VALUES ('late-foreign-run')")
    result = recent(storage, log_dir, managed_for(storage, RUN_A))
    assert result["index_state"]["status"] == "ready" and sorted(hit_ids(result)) == ["a-hit", "b-hit", "d-hit"]


def test_an_unreadable_authorized_descendant_is_listed_as_unavailable(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    storage.update_run(RUN_B, lambda run: run["activations"].append(link("child-broken", "broken")), "link")
    (storage.runs / "child-broken.json").write_text("{")
    (storage.runs / "foreign-broken.json").write_text("{")
    result = settle(storage, log_dir, managed_for(storage, RUN_B))
    assert [item["workflow_run_id"] for item in result["unavailable"]] == ["child-broken"] and result["scan"]["complete"] is False
    assert sorted(hit_ids(result)) == ["b-hit", "d-hit"] and "foreign-broken" not in json.dumps(result)


def test_a_named_child_without_a_verifiable_edge_makes_the_answer_incomplete(storage, clock, repos, log_dir):
    tree(storage, clock, repos)
    # No monkeypatch: the parent holds no pinned dependency edge for its child, so nothing beyond the own run is shown.
    result = settle(storage, log_dir, managed_for(storage, RUN_A))
    assert hit_ids(result) == ["a-hit"] and result["current"] == []
    assert result["scan"] == {"examined": 1, "limit": 500, "current_examined": 0, "current_limit": 100, "complete": False}


def test_edge_verification_checks_parent_execution_root_and_pinned_edge():
    parent = {"workflow_run_id": "p", "workflow_id": "wf-p", "dependency_tree": {"edges": [{"from": "wf-p", "node_id": "n", "to": "wf-c"}]}}
    activation = {"id": "e1"}
    child = {"workflow_run_id": "c", "workflow_id": "wf-c", "parent_link": {"workflow_run_id": "p", "root_workflow_run_id": "p", "execution_id": "e1", "node_id": "n"}}
    assert workflow_limit_hits._edge_verified(parent, activation, child, "p")
    for change in ({"workflow_run_id": "other"}, {"execution_id": "e2"}, {"root_workflow_run_id": "elsewhere"}, {"node_id": "m"}):
        assert not workflow_limit_hits._edge_verified(parent, activation, {**child, "parent_link": {**child["parent_link"], **change}}, "p"), change
    assert not workflow_limit_hits._edge_verified(parent, activation, {**child, "workflow_id": "wf-other"}, "p")
    assert not workflow_limit_hits._edge_verified({**parent, "dependency_tree": {}}, activation, child, "p")


def test_children_are_read_once_with_the_loader_byte_bound_and_oversized_ones_are_unavailable(storage, clock, repos, log_dir, verified, monkeypatch):
    tree(storage, clock, repos)
    caller = managed_for(storage, RUN_A)
    real, limits = workflows.WorkflowStore.get_run, []

    def bounded(self, run_id, **kwargs):
        limits.append((run_id, kwargs.get("metadata_byte_limit")))
        if run_id == RUN_B:
            from harflow_mcp.bounded_io import ReadLimit
            raise ReadLimit("JSON exceeds bounded read budget")
        return real(self, run_id, **kwargs)

    monkeypatch.setattr(workflows.WorkflowStore, "get_run", bounded)
    result = recent(storage, log_dir, caller)
    assert [item["workflow_run_id"] for item in result["unavailable"]] == [RUN_B] and result["scan"]["complete"] is False
    assert limits and all(limit == workflow_limit_hits.READ_BYTES for run_id, limit in limits if run_id == RUN_B)
    assert hit_ids(result) == ["a-hit"]


def test_a_repaired_blocked_child_recovers(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    caller = managed_for(storage, RUN_A)
    path = storage.runs / f"{RUN_B}.json"
    good = path.read_text()
    path.write_text("{")
    broken = settle(storage, log_dir, caller)
    assert [item["workflow_run_id"] for item in broken["unavailable"]] == [RUN_B] and broken["scan"]["complete"] is False
    path.write_text(good)
    fixed = settle(storage, log_dir, caller)
    assert fixed["unavailable"] == [] and sorted(hit_ids(fixed)) == ["a-hit", "b-hit", "d-hit"] and fixed["scan"]["complete"] is True


def test_a_document_changed_in_place_is_requeued_and_then_included(storage, clock, repos, log_dir, verified, monkeypatch):
    tree(storage, clock, repos)
    caller = managed_for(storage, RUN_A)
    assert sorted(hit_ids(settle(storage, log_dir, caller))) == ["a-hit", "b-hit", "d-hit"]
    path = storage.runs / f"{RUN_B}.json"
    mtime = storage.runs.stat().st_mtime_ns
    document = json.loads(path.read_text())
    document["summary"] = "edited in place " * 3
    path.write_text(json.dumps(document))
    assert storage.runs.stat().st_mtime_ns == mtime  # discovery will not revisit the directory
    real = workflows.WorkflowStore.prepare_run_catalog
    monkeypatch.setattr(workflows.WorkflowStore, "prepare_run_catalog", lambda self: {})
    first = recent(storage, log_dir, caller)
    assert first["index_state"]["status"] == "preparing" and first["latest"] == [] and first["scan"]["complete"] is False
    monkeypatch.setattr(workflows.WorkflowStore, "prepare_run_catalog", real)
    later = recent(storage, log_dir, caller)
    assert sorted(hit_ids(later)) == ["a-hit", "b-hit", "d-hit"] and later["scan"]["complete"] is True


def test_unverified_descendants_cannot_exhaust_a_managed_window_or_caps(storage, clock, repos, log_dir, monkeypatch):
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a-hit")), created=BASE)
    for number in range(6):
        tasks = [attempt(f"u{number}-{index:03}", finished=BASE + 100 + number * 100 + index) for index in range(100)]
        seed(storage, clock, repos["alpha"], f"unverified-{number:04}", execution("e1", "work", *tasks), parent=RUN_A, created=BASE + 1 + number,
             worker_usage_recovery=worker_recovery(), status="needs_input")
    result = settle(storage, log_dir, managed_for(storage, RUN_A))
    assert hit_ids(result) == ["a-hit"] and result["scan"]["examined"] == 1 and result["scan"]["complete"] is True and result["current"] == []


def test_catalog_links_not_named_by_the_callers_invocations_do_not_displace_a_descendant(storage, clock, repos, log_dir, monkeypatch):
    monkeypatch.setattr(workflow_limit_hits, "TREE_LIMIT", 3)
    bad = [f"unnamed-{number:04}" for number in range(6)]
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a-hit")), link(RUN_B), created=BASE)
    for number, child in enumerate(bad):
        seed(storage, clock, repos["alpha"], child, execution("e1", "work", attempt(f"bad-{number}", model="gpt-bad")), parent=RUN_A, created=BASE + 1 + number)
    seed(storage, clock, repos["alpha"], RUN_B, execution("e1", "work", attempt("b-hit", model="gpt-b")), parent=RUN_A, created=BASE + 20)
    monkeypatch.setattr(workflow_limit_hits, "_edge_verified", lambda parent, activation, child, root: True)
    result = settle(storage, log_dir, managed_for(storage, RUN_A))
    assert sorted(hit_ids(result)) == ["a-hit", "b-hit"] and result["scan"]["complete"] is True and "bad-" not in json.dumps(result)


def test_verified_members_beyond_the_cap_make_the_answer_incomplete(storage, clock, repos, log_dir, verified, monkeypatch):
    monkeypatch.setattr(workflow_limit_hits, "TREE_LIMIT", 2)
    seed(storage, clock, repos["alpha"], RUN_A, execution("e1", "work", attempt("a-hit")), link(RUN_B), link(RUN_C), created=BASE)
    seed(storage, clock, repos["alpha"], RUN_B, execution("e1", "work", attempt("b-hit", model="gpt-b")), parent=RUN_A, created=BASE + 1)
    seed(storage, clock, repos["alpha"], RUN_C, execution("e1", "work", attempt("c-hit", model="gpt-c")), parent=RUN_A, created=BASE + 2)
    result = settle(storage, log_dir, managed_for(storage, RUN_A))
    assert sorted(hit_ids(result)) == ["a-hit", "b-hit"] and result["scan"]["complete"] is False


def unindex(storage, run_id):
    index, _ = storage.limit_hit_catalog()
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        index._remove(db, run_id)
        db.execute("INSERT OR IGNORE INTO pending VALUES (?)", (run_id,))


def test_a_named_child_that_is_only_pending_is_resolved_before_the_answer_is_complete(storage, clock, repos, log_dir, verified):
    tree(storage, clock, repos)
    unindex(storage, RUN_B)
    caller = managed_for(storage, RUN_A)
    result = recent(storage, log_dir, caller)  # the first attempt finds it pending, indexes one slice and retries
    assert sorted(hit_ids(result)) == ["a-hit", "b-hit", "d-hit"] and result["scan"]["complete"] is True


def test_a_named_child_that_stays_unresolved_gives_a_sanitized_preparing_answer(storage, clock, repos, log_dir, verified, monkeypatch):
    tree(storage, clock, repos)
    unindex(storage, RUN_B)
    monkeypatch.setattr(workflows.WorkflowStore, "prepare_run_catalog", lambda self: {})
    result = recent(storage, log_dir, managed_for(storage, RUN_A))
    assert result["scan"]["complete"] is False and result["latest"] == [] and result["current"] == []
    assert set(result["index_state"]) == {"status", "source", "reason"} and result["index_state"]["status"] == "preparing"


@pytest.mark.parametrize("role", ["node", "builder"])
def test_workers_and_builders_are_refused_before_any_lookup(storage, clock, repos, log_dir, monkeypatch, role):
    tree(storage, clock, repos)
    monkeypatch.setattr(catalog.Catalog, "connect", lambda *a, **k: pytest.fail("catalog opened before refusal"))
    monkeypatch.setattr(workflows.WorkflowStore, "child_states", lambda *a, **k: pytest.fail("tree read before refusal"))
    with pytest.raises(workflow_limit_hits.Refused, match="workers and builders"):
        workflow_limit_hits.recent(log_dir, (association(role), {"workflow_run_id": RUN_A}), {})

    async def not_called():
        monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda directory: (True, (association(role), {"workflow_run_id": RUN_A})))
        monkeypatch.setattr(id_resolution, "resolve_run", lambda *a, **k: pytest.fail("resolution before refusal"))
        await workflow_reads.call("limit_hits", directory=log_dir, filters={"root_workflow_run_id": RUN_A})

    import asyncio
    with pytest.raises(workflow_limit_hits.Refused):
        asyncio.run(not_called())


def test_an_undecidable_caller_is_refused_before_any_lookup(log_dir, monkeypatch):
    import asyncio

    def undecidable(directory):
        raise ValueError("Workflow caller authority is undecidable: ps unavailable")

    monkeypatch.setattr(workflow_inspection, "managed_page_reader", undecidable)
    monkeypatch.setattr(catalog.Catalog, "connect", lambda *a, **k: pytest.fail("catalog opened before refusal"))
    with pytest.raises(ValueError, match="undecidable"):
        asyncio.run(workflow_reads.call("limit_hits", directory=log_dir, filters={}))


def test_authority_that_is_not_ready_exposes_nothing(storage, clock, repos, log_dir, monkeypatch):
    import asyncio
    tree(storage, clock, repos)
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda directory: (False, None))
    result = asyncio.run(workflow_reads.call("limit_hits", directory=log_dir, filters={}))
    assert result["latest"] == [] and result["current"] == [] and result["scan"]["complete"] is False and result["index_state"]["status"] in {"preparing", "blocked"}
    assert set(result["index_state"]) <= {"status", "source", "reason"}
    assert "blocked_sources" not in json.dumps(result) and "pending_records" not in json.dumps(result)


# ---- MCP --------------------------------------------------------------------------------------------

async def test_mcp_tool_returns_hits_and_resolves_a_root_prefix(storage, clock, repos):
    two_workflows(storage, clock, repos)
    result = await server.workflow_limit_hits()
    assert result["index_state"]["status"] in {"ready", "preparing"}
    for _ in range(4):
        result = await server.workflow_limit_hits(workflow_id="wf-b")
    assert sorted(hit_ids(result)) == ["b1", "c1"] and [item["workflow_run_id"] for item in result["current"]] == [RUN_C]
    prefixed = await server.workflow_limit_hits(root_workflow_run_id=RUN_B[:10])
    assert sorted(hit_ids(prefixed)) == ["b1", "c1"] and prefixed["resolved_from_root_workflow_run_id"] == {"input": RUN_B[:10], "workflow_run_id": RUN_B}
    assert (await server.workflow_limit_hits(current_only=True))["latest"] == []


async def test_mcp_tool_rejects_bad_filters_as_invalid_params(storage, clock, repos):
    two_workflows(storage, clock, repos)
    for filters in ({"since": "yesterday"}, {"workflow_id": " "}, {"root_workflow_run_id": "nope-nope-nope"}):
        with pytest.raises(MCPError) as caught:
            await server.workflow_limit_hits(**filters)
        assert caught.value.error.code == -32602


async def test_mcp_tool_maps_a_worker_refusal_to_invalid_params_while_ctl_uses_workflow_read_refused(storage, clock, repos, monkeypatch):
    two_workflows(storage, clock, repos)
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda directory: (True, (association("node"), {"workflow_run_id": RUN_A})))
    with pytest.raises(MCPError, match="workers and builders") as caught:
        await server.workflow_limit_hits()
    assert caught.value.error.code == -32602


async def test_mcp_tool_is_registered_with_documented_arguments():
    from mcp import Client
    async with Client(server.mcp) as client:
        tools = {tool.name: tool for tool in (await client.list_tools()).tools}
    tool = tools["workflow_limit_hits"]
    assert set(tool.input_schema["properties"]) == {"workflow_id", "repo", "since", "before", "root_workflow_run_id", "current_only"}
    description = " ".join(tool.description.split())
    assert "execution:<id>" in description and "workers and builders are refused" in description and "does not say the provider is available" in description


# ---- ctl and contract fixtures ----------------------------------------------------------------------

@pytest.fixture
def ctl_state(storage, log_dir, monkeypatch):
    monkeypatch.setattr(ctl, "default_log_dir", lambda: log_dir)
    monkeypatch.setattr(takeover, "caller_refusal", lambda *a, **k: None)
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda directory: (True, None))
    return log_dir


def run_ctl(capsys, *arguments):
    code = ctl.main(["workflow-limit-hits", *arguments, "--json"])
    return code, json.loads(capsys.readouterr().out)


def scenario_ready(storage, clock, repos):
    fixture_a, fixture_b, fixture_c = "fixture-root-0001", "fixture-child-0002", "fixture-other-0003"
    seed(storage, clock, repos["alpha"], fixture_a, execution("exec-1", "implement", attempt("fixture-codex-new", observed="gpt-x-2026", finished=BASE + 3600,
                                                                                                failure=diagnostic(http_status=429, provider_code="usage_limit_reached", reset_at=FUTURE)),
                                                              attempt("fixture-codex-old", model="gpt-y", finished=BASE + 60, failure=diagnostic(reset_at=PAST))),
         execution("exec-2", "review", attempt("fixture-claude", backend="claude", model=None, finished=BASE + 120, failure=diagnostic(source="result")), role="node"),
         workflow_id="wf-fixture", status="running", created=BASE,
         usage_recovery_history=[{"decision_id": "d-old", "role": "node", "accepted_answer": "fallback", "accepted_at": BASE + 90, "failure_diagnostic": diagnostic(reset_at=PAST),
                                  "failed_candidate": {"backend": "codex", "model": "gpt-y"}}])
    seed(storage, clock, repos["alpha"], fixture_b, execution("exec-1", "implement", attempt("fixture-child", model="gpt-c", finished=BASE + 7200, failure=diagnostic(reset_at=1_600_000_000))),
         workflow_id="wf-fixture", status="needs_input", parent=fixture_a, created=BASE + 1, worker_usage_recovery=worker_recovery(decision_id="fixture-decision", node_id="implement", execution_id="exec-1"))
    seed(storage, clock, repos["beta"], fixture_c, execution("exec-1", "implement", attempt("fixture-plain", failure=False)), workflow_id="wf-other", created=BASE + 2)


def scenario_empty(storage, clock, repos):
    seed(storage, clock, repos["alpha"], "fixture-plain-0001", execution("exec-1", "implement", attempt("fixture-ok", failure=False)), workflow_id="wf-fixture", status="completed")


def scenario_preparing(storage, clock, repos):
    """101 retained runs, the first and last carrying hits; a cold catalog indexes 100 per read."""
    for number in range(101):
        tasks = [attempt(f"p{number:04}", model="gpt-x" if number == 0 else "gpt-y", finished=BASE + number)] if number in (0, 100) else []
        seed(storage, clock, repos["alpha"], f"prepare-{number:04}", execution("exec-1", "implement", *tasks), workflow_id="wf-fixture", created=BASE + number)
    for suffix in ("", "-shm", "-wal"):
        Path(f"{catalog.catalog_path(storage.runs)}{suffix}").unlink(missing_ok=True)


FIXTURE_SCENARIOS = {
    "ctl-workflow-limit-hits-ready.json": scenario_ready,
    "ctl-workflow-limit-hits-empty.json": scenario_empty,
    "ctl-workflow-limit-hits-preparing.json": scenario_preparing,
}


def normalized(document):
    """Fixtures carry no wall-clock or path-dependent values; this only guards that assumption."""
    text = json.dumps(document)
    assert "/private" not in text and "pytest" not in text
    return document


@pytest.mark.parametrize("name", sorted(FIXTURE_SCENARIOS))
def test_ctl_output_matches_the_canonical_fixture(name, storage, clock, repos, ctl_state, capsys):
    FIXTURE_SCENARIOS[name](storage, clock, repos)
    if name != "ctl-workflow-limit-hits-preparing.json":
        for _ in range(4):
            code, document = run_ctl(capsys)
            if document["result"]["index_state"]["status"] == "ready":
                break
    else:
        code, document = run_ctl(capsys)
    assert code == 0
    assert normalized(document) == json.loads((FIXTURES / name).read_text())


@pytest.mark.parametrize("name", sorted(FIXTURE_SCENARIOS))
def test_workflow_limit_hit_fixtures_are_manifested_and_wire_valid(name):
    manifest = json.loads((FIXTURES / "manifest.json").read_text())
    assert manifest["cases"][name] == {"encoding": "json", "expected_product_version": wire_contracts.PRODUCT_VERSION, "format": CTL_FORMAT,
                                       "required_capabilities": [], "valid": True}
    assert manifest["files"][name] == hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest()
    document = json.loads((FIXTURES / name).read_text())
    validate_envelope(document, CTL_FORMAT)
    assert set(document["result"]) == {"current", "latest", "unavailable", "scan", "index_state"}


def test_fixtures_cover_blocking_recovery_reset_states_and_incomplete_flags():
    ready = json.loads((FIXTURES / "ctl-workflow-limit-hits-ready.json").read_text())["result"]
    assert {hit["reset"]["state"] for hit in ready["latest"]} == {"future", "elapsed", "unknown"}
    assert any("raw" not in hit["reset"] for hit in ready["latest"]) and any("recovery" in hit for hit in ready["latest"])
    assert ready["current"] and ready["current"][0]["workflow_run_id"] != ready["current"][0]["root_workflow_run_id"]
    assert all(hit["detail_ref"]["view"].startswith("execution:") for hit in ready["latest"]) and ready["scan"]["complete"] is True
    empty = json.loads((FIXTURES / "ctl-workflow-limit-hits-empty.json").read_text())["result"]
    assert empty["latest"] == [] and empty["current"] == [] and empty["scan"]["complete"] is True
    preparing = json.loads((FIXTURES / "ctl-workflow-limit-hits-preparing.json").read_text())["result"]
    assert preparing["scan"]["complete"] is False and preparing["index_state"]["status"] == "preparing"


def test_ctl_filters(storage, clock, repos, ctl_state, capsys):
    two_workflows(storage, clock, repos)
    for _ in range(4):
        run_ctl(capsys)
    code, document = run_ctl(capsys, "--workflow-id", "wf-a", "--repo", str(repos["alpha"].resolve()), "--since", "2023-01-01T00:00:00Z", "--before", "2099-01-01T00:00:00Z")
    assert code == 0 and hit_ids(document["result"]) == ["a1"]
    code, document = run_ctl(capsys, "--root-workflow-run-id", RUN_B[:10], "--current-only")
    assert hit_ids(document["result"]) == [] and [item["workflow_run_id"] for item in document["result"]["current"]] == [RUN_C]
    assert document["result"]["resolved_from_root_workflow_run_id"]["workflow_run_id"] == RUN_B
    assert ctl.main(["workflow-limit-hits"]) == 0
    assert json.loads(capsys.readouterr().out)["scan"]["limit"] == 500


def test_ctl_invalid_filter_is_invalid_params(storage, clock, repos, ctl_state, capsys):
    scenario_empty(storage, clock, repos)
    code, document = run_ctl(capsys, "--since", "yesterday")
    assert code == 1 and document["error"]["code"] == "invalid_params" and "since" in document["error"]["message"]


def test_ctl_reports_an_unreadable_catalog_as_workflow_limit_hits_error(storage, clock, repos, ctl_state, capsys, monkeypatch):
    scenario_empty(storage, clock, repos)

    def unavailable(*args, **kwargs):
        raise OSError("disk unavailable")

    monkeypatch.setattr(workflow_limit_hits, "recent", unavailable)
    code, document = run_ctl(capsys)
    assert code == 1 and document["error"]["code"] == "workflow_limit_hits_error" and "disk unavailable" in document["error"]["message"]
    monkeypatch.setattr(workflow_limit_hits, "recent", lambda *a, **k: (_ for _ in ()).throw(sqlite3.OperationalError("locked")))
    assert run_ctl(capsys)[1]["error"]["code"] == "workflow_limit_hits_error"


def test_ctl_refuses_a_worker_before_any_lookup(storage, clock, repos, ctl_state, capsys, monkeypatch):
    scenario_empty(storage, clock, repos)
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda directory: (True, (association("node"), {"workflow_run_id": RUN_A})))
    monkeypatch.setattr(catalog.Catalog, "connect", lambda *a, **k: pytest.fail("catalog opened before refusal"))
    code, document = run_ctl(capsys)
    assert code == 1 and document["error"]["code"] == "workflow_read_refused" and "workers and builders" in document["error"]["message"]


def test_ctl_command_is_read_only_and_needs_no_mutation_authority(storage, clock, repos, ctl_state, capsys, monkeypatch):
    from harflow_mcp import caller_policy
    scenario_empty(storage, clock, repos)
    monkeypatch.setattr(caller_policy, "require_mutation", lambda *a, **k: pytest.fail("workflow-limit-hits is read only"))
    assert "workflow-limit-hits" in workflow_reads.READ_ONLY_CLI_COMMANDS
    assert run_ctl(capsys)[0] == 0


def test_capability_is_advertised_by_the_engine_and_the_canonical_handshake_fixture():
    assert "workflow-limit-hits.v1" in wire_contracts.CAPABILITIES
    assert "workflow-limit-hits.v1" in wire_contracts.version_info()["capabilities"]
    document = json.loads((FIXTURES / "version-info.json").read_text())
    assert document["capabilities"] == list(wire_contracts.CAPABILITIES)


def test_unavailable_query_uses_the_incomplete_header_index(storage):
    traced = traced_statements_any(storage, lambda index, db: index.incomplete_sources(db, 20))
    assert traced and all(any("USING COVERING INDEX incomplete_headers" in detail or "USING INDEX incomplete_headers" in detail for detail in plan) for plan in traced.values()), traced


def traced_statements_any(storage, call):
    index, _ = storage.limit_hit_catalog()
    statements = []
    with state_ownership.state_session(storage.state_layout), index.connect() as db:
        db.set_trace_callback(statements.append)
        call(index, db)
        db.set_trace_callback(None)
        return {statement: [detail for *_, detail in db.execute("EXPLAIN QUERY PLAN " + statement)] for statement in statements if "needs_direct_lookup" in statement}
