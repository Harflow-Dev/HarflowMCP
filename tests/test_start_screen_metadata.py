"""Isolated regressions for Start Screen lifecycle and ownership metadata.

Tests use temporary owned state and fake timestamps; none launch workers or call agent services.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from pathlib import Path

import pytest

from harflow_mcp import bounded_io, catalog, store as task_store, workflow_references
from harflow_mcp import workflow_receipt_documents, workflow_run_documents, workflow_responses, workflows
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp import state_ownership
from harflow_mcp.wire_contracts import CTL_FORMAT, validate_envelope


def _definition(name: str = "metadata-fixture", workflow_id: str = "workflow-generation-1") -> dict:
    value = {
        "name": name,
        "workflow_id": workflow_id,
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "role": "task", "instructions": "Fixture", "agent": {"backend": "codex"}},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }
    return workflows.validate_definition(value)


@pytest.fixture
def owned_state(tmp_path):
    root = tmp_path / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


@pytest.fixture
def workflow_store(owned_state, tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda _backend: True)
    return workflows.WorkflowStore(root=owned_state), tmp_path


def _create_run(workflow_store):
    storage, repo = workflow_store
    return storage.create_run(_definition(), "fixture prompt", repo)


def _task(identifier: str, *, finished_at: str | None = "2026-10-27T00:00:00+00:00", **fields):
    return task_store.TaskRecord(
        identifier,
        "codex",
        "fixture-session",
        "/fixture/repository",
        "2026-10-27T00:00:00+00:00",
        status="completed",
        exit_code=0,
        finished_at=finished_at,
        **fields,
    )


def _task_page(root: Path, task_id: str) -> tuple[dict, dict]:
    page = task_store.list_page(root / "tasks", task_ids=[task_id])
    return page, page["items"][0]


def _write_receipt(root: Path, task_id: str, run_id: str) -> None:
    path = root / "workflow-owners" / f"{task_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(workflow_receipt_documents.encode({"workflow_run_id": run_id}, task_id)), encoding="utf-8")


class _Clock:
    def __init__(self, value: float):
        self.value = value

    def time(self) -> float:
        return self.value


def test_create_run_initializes_nullable_finished_at_and_run_codec_accepts_legacy_omission(workflow_store):
    run = _create_run(workflow_store)

    assert run["finished_at"] is None
    assert run["finished_at_sequence"] == run["sequence"] == 0
    encoded = workflow_run_documents.encode(run)
    assert workflow_run_documents.decode(encoded, run["workflow_run_id"])["finished_at"] is None

    legacy = copy.deepcopy(encoded)
    legacy["run"].pop("finished_at")
    decoded = workflow_run_documents.decode(legacy, run["workflow_run_id"])
    assert "finished_at" not in decoded


@pytest.mark.parametrize("value", [True, "2026-10-27T00:00:00Z", math.nan, math.inf, -math.inf, [], {}])
def test_workflow_run_codec_rejects_nonnullable_nonfinite_or_nonnumeric_finished_at(workflow_store, value):
    run = copy.deepcopy(_create_run(workflow_store))
    run["finished_at"] = value

    with pytest.raises(workflow_run_documents.WorkflowRunDocumentError):
        workflow_run_documents.encode(run)


@pytest.mark.parametrize("value", [True, -1, 1.0, "1", None, [], {}])
def test_workflow_run_codec_rejects_invalid_finish_witness(workflow_store, value):
    run = copy.deepcopy(_create_run(workflow_store))
    run["finished_at_sequence"] = value

    with pytest.raises(workflow_run_documents.WorkflowRunDocumentError):
        workflow_run_documents.encode(run)


def test_terminal_transition_stamps_once_and_reopen_then_refinish_stamps_a_new_time(workflow_store, monkeypatch):
    storage, _ = workflow_store
    run = _create_run(workflow_store)
    clock = _Clock(100.0)
    monkeypatch.setattr(workflows, "time", clock)

    first = storage.update_run(run["workflow_run_id"], lambda row: row.update(status="completed"), "fixture_transition")
    assert first["finished_at"] == 100.0
    assert first["updated_at"] >= first["finished_at"]

    clock.value = 200.0
    cleanup = storage.update_run(run["workflow_run_id"], lambda row: row.update(summary="cleanup"), "fixture_terminal_cleanup")
    assert cleanup["finished_at"] == 100.0
    assert cleanup["updated_at"] == 200.0

    clock.value = 300.0
    reopened = storage.update_run(run["workflow_run_id"], lambda row: row.update(status="running"), "fixture_reopen")
    assert reopened["finished_at"] is None

    clock.value = 400.0
    finished_again = storage.update_run(run["workflow_run_id"], lambda row: row.update(status="failed"), "fixture_refinish")
    assert finished_again["finished_at"] == 400.0
    assert finished_again["updated_at"] >= finished_again["finished_at"]


@pytest.mark.parametrize("task_status", ["reserved", "running", "uncertain"])
def test_cached_workflow_header_separates_finish_time_from_child_settling(workflow_store, monkeypatch, task_status):
    storage, _ = workflow_store
    run = _create_run(workflow_store)
    clock = _Clock(1_700_000_000.0)
    monkeypatch.setattr(workflows, "time", clock)
    activation = {
        "id": "execution",
        "node_id": "work",
        "role": "node",
        "status": "running",
        "created_at": 1_699_999_000.0,
        "tasks": [{"task_id": "settling-task", "status": task_status}],
    }

    terminal = storage.update_run(
        run["workflow_run_id"],
        lambda row: row.update(status="completed", activations=[activation]),
        "fixture_terminal_transition",
    )
    assert terminal["finished_at"] == 1_700_000_000.0
    assert terminal["settling"]
    real_get_run = storage.get_run
    monkeypatch.setattr(storage, "get_run", lambda *_args, **_kwargs: pytest.fail("cached header must not materialize the run"))
    header = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
    assert header["finished_at"] == 1_700_000_000.0
    assert header["settling"] is True

    monkeypatch.setattr(storage, "get_run", real_get_run)
    clock.value = 1_700_000_010.0
    settled = storage.update_run(
        run["workflow_run_id"],
        lambda row: row.update(activations=[{**activation, "status": "completed", "tasks": [{"task_id": "settling-task", "status": "completed"}]}]),
        "fixture_settled_cleanup",
    )
    assert not settled["settling"]
    assert settled["finished_at"] == 1_700_000_000.0
    assert settled["updated_at"] == 1_700_000_010.0
    monkeypatch.setattr(storage, "get_run", lambda *_args, **_kwargs: pytest.fail("settled cached header must not materialize the run"))
    settled_header = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
    assert settled_header["settling"] is False
    assert settled_header["finished_at"] == 1_700_000_000.0


@pytest.mark.parametrize("terminal", ["completed", "failed", "cancelled", "timed_out"])
def test_each_terminal_status_transition_records_the_transition_time(workflow_store, monkeypatch, terminal):
    storage, _ = workflow_store
    run = _create_run(workflow_store)
    clock = _Clock(1_700_000_000.25)
    monkeypatch.setattr(workflows, "time", clock)

    updated = storage.update_run(run["workflow_run_id"], lambda row: row.update(status=terminal), "fixture_transition")

    assert updated["finished_at"] == 1_700_000_000.25
    assert updated["updated_at"] >= updated["finished_at"]


def test_legacy_terminal_without_finish_time_stays_unknown_after_bookkeeping(workflow_store, monkeypatch):
    storage, _ = workflow_store
    run = _create_run(workflow_store)
    legacy = copy.deepcopy(run)
    legacy["status"] = "completed"
    legacy.pop("finished_at", None)
    source = storage.runs / f"{run['workflow_run_id']}.json"
    source.write_text(json.dumps(workflow_run_documents.encode(legacy)), encoding="utf-8")
    clock = _Clock(1_800_000_000.0)
    monkeypatch.setattr(workflows, "time", clock)

    cleaned = storage.update_run(run["workflow_run_id"], lambda row: row.update(summary="terminal cleanup"), "fixture_cleanup")

    assert cleaned.get("finished_at") is None
    assert cleaned["updated_at"] == 1_800_000_000.0


def test_legacy_numeric_finish_without_witness_is_never_projected_as_known(workflow_store):
    storage, _ = workflow_store
    run = _create_run(workflow_store)
    path = storage.runs / f"{run['workflow_run_id']}.json"
    document = workflow_run_documents.encode(run)
    document["run"].update(status="completed", finished_at=1_700_000_000.0, sequence=12)
    document["run"].pop("finished_at_sequence", None)
    path.write_text(json.dumps(document), encoding="utf-8")
    original = path.read_bytes()

    loaded = storage.get_run(run["workflow_run_id"])

    assert loaded["finished_at"] is None
    assert workflow_responses.compact(loaded)["finished_at"] is None
    assert storage.run_header(loaded)["finished_at"] is None
    assert path.read_bytes() == original


def test_old_writer_sequence_changes_invalidate_finish_until_new_transition(workflow_store, monkeypatch):
    storage, _ = workflow_store
    run = _create_run(workflow_store)
    clock = _Clock(100.0)
    monkeypatch.setattr(workflows, "time", clock)
    finished = storage.update_run(run["workflow_run_id"], lambda row: row.update(status="completed"), "finish")
    assert finished["finished_at"] == 100.0
    assert finished["finished_at_sequence"] == finished["sequence"]
    persisted = storage.runs / f"{run['workflow_run_id']}.json"

    def emulate_old_writer(status: str, timestamp: float) -> None:
        document = json.loads(persisted.read_text(encoding="utf-8"))
        raw_run = document["run"]
        raw_run["status"] = status
        raw_run["sequence"] += 1
        raw_run["updated_at"] = timestamp
        persisted.write_text(json.dumps(document), encoding="utf-8")

    # An older writer preserves the unknown additive field but advances sequence.
    emulate_old_writer("running", 200.0)
    stale_bytes = persisted.read_bytes()
    reopened = storage.get_run(run["workflow_run_id"])
    assert reopened["finished_at"] is None
    assert workflow_responses.compact(reopened)["finished_at"] is None
    assert storage.run_header(reopened)["finished_at"] is None
    assert persisted.read_bytes() == stale_bytes

    # Refinalizing under the old writer leaves a numeric timestamp but no matching witness.
    emulate_old_writer("completed", 300.0)
    stale_terminal = storage.get_run(run["workflow_run_id"])
    assert stale_terminal["finished_at"] is None
    assert workflow_responses.compact(stale_terminal)["finished_at"] is None
    assert storage.run_header(stale_terminal)["finished_at"] is None

    # A new bookkeeping mutation must not bless that stale numeric timestamp.
    clock.value = 400.0
    cleaned = storage.update_run(run["workflow_run_id"], lambda row: row.update(summary="cleanup"), "cleanup")
    assert cleaned["finished_at"] is None
    assert cleaned["finished_at_sequence"] == cleaned["sequence"]
    assert workflow_responses.compact(cleaned)["finished_at"] is None

    # Only a newly observed nonterminal-to-terminal transition restores evidence.
    storage.update_run(run["workflow_run_id"], lambda row: row.update(status="running"), "reopen")
    clock.value = 500.0
    fresh = storage.update_run(run["workflow_run_id"], lambda row: row.update(status="timed_out"), "refinish")
    assert fresh["finished_at"] == 500.0
    assert fresh["finished_at_sequence"] == fresh["sequence"]
    assert fresh["updated_at"] >= fresh["finished_at"]


@pytest.mark.parametrize(
    "change",
    [
        {"finished_at_sequence": True},
        {"finished_at_sequence": -1},
        {"finished_at_sequence": 4},
        {"sequence": True},
        {"sequence": -1},
        {"finished_at": True},
        {"finished_at": math.nan},
        {"finished_at": math.inf},
    ],
)
def test_finish_projection_requires_terminal_status_finite_time_and_exact_matching_witness(change):
    from harflow_mcp.workflow_completion import qualified_finished_at

    run = {"status": "completed", "finished_at": 100.5, "sequence": 5, "finished_at_sequence": 5}

    assert qualified_finished_at(run) == 100.5
    assert qualified_finished_at(run | change) is None
    assert qualified_finished_at(run | {"status": "running"}) is None


def test_child_run_initializes_finish_witness_at_creation(workflow_store, monkeypatch):
    from harflow_mcp import workflow_goals, workflow_invocation
    from harflow_mcp import workflow_native_policy
    from harflow_mcp.workflow_references import resolve_dependencies

    storage, _ = workflow_store
    child_definition = storage.save("child-definition", _definition("child-definition"))
    pinned_tree = resolve_dependencies(storage, definition=child_definition)
    parent = _create_run(workflow_store)
    parent = storage.update_run(
        parent["workflow_run_id"],
        lambda run: run.update(dependency_tree=pinned_tree),
        "pin_child_fixture",
    )
    monkeypatch.setattr(workflow_goals, "context", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(workflow_native_policy, "child_contracts", lambda *_args, **_kwargs: None)

    child = workflow_invocation.child_run_record(
        storage,
        parent,
        {"id": "child-execution"},
        {"id": "call-child", "workflow_ref": {"workflow_id": child_definition["workflow_id"]}},
        {"child_workflow_run_id": "child-run-fixture", "assignment": "fixture"},
    )

    assert child["finished_at"] is None
    assert child["sequence"] == child["finished_at_sequence"] == 0


def test_compact_projects_pinned_workflow_identity_and_finished_at_not_current_saved_identity(workflow_store):
    storage, repo = workflow_store
    saved = storage.save("same-name", _definition("same-name"))
    run = storage.create_run(saved, "fixture", repo)
    pinned_id = run["definition"]["workflow_id"]
    storage.delete("same-name")
    recreated = storage.save("same-name", _definition("same-name"))

    response = workflow_responses.compact(storage.get_run(run["workflow_run_id"]))

    assert recreated["workflow_id"] != pinned_id
    assert response["workflow_id"] == pinned_id
    assert response["finished_at"] is None
    assert workflow_references.workflow_identity(run["definition"]) == response["workflow_id"]


def test_compact_uses_deterministic_legacy_identity_for_legacy_definition():
    run = {
        "workflow_run_id": "legacy-run",
        "name": "legacy saved workflow",
        "kind": "workflow",
        "status": "completed",
        "definition": {"name": "legacy saved workflow"},
        "finished_at": None,
    }

    response = workflow_responses.compact(run)

    assert response["workflow_id"] == workflow_references.workflow_identity(run["definition"])
    assert response["workflow_id"].startswith("legacy-")


def test_builder_header_does_not_claim_saved_workflow_identity(workflow_store):
    storage, repo = workflow_store
    run = storage.create_run(_definition(workflow_id="builder-descriptor-id"), "fixture", repo, kind="builder")

    header = storage.run_header(storage.get_run(run["workflow_run_id"]))

    assert "workflow_id" not in workflow_responses.compact(run)
    assert "workflow_id" not in header


def test_standalone_task_page_has_explicit_null_owner(owned_state):
    task_store.write(owned_state / "tasks", _task("standalone-task"))

    _, item = _task_page(owned_state, "standalone-task")

    assert "workflow_run_id" in item
    assert item["workflow_run_id"] is None


def test_owned_task_page_has_valid_receipt_owner(workflow_store, owned_state):
    run = _create_run(workflow_store)
    task_id = "owned-task"
    task_store.write(owned_state / "tasks", _task(task_id))
    _write_receipt(owned_state, task_id, run["workflow_run_id"])

    _, item = _task_page(owned_state, task_id)

    assert item["workflow_run_id"] == run["workflow_run_id"]


@pytest.mark.parametrize("receipt_kind", ["malformed", "permission-denied"])
def test_uncertain_or_unreadable_receipt_omits_owner_and_marks_counts_unknown(owned_state, monkeypatch, receipt_kind):
    task_id = "uncertain-task"
    task_store.write(owned_state / "tasks", _task(task_id))
    receipt = owned_state / "workflow-owners" / f"{task_id}.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    if receipt_kind == "malformed":
        receipt.write_text("{not-json", encoding="utf-8")
    else:
        def unreadable(*_args, **_kwargs):
            raise PermissionError("fixture receipt is unreadable")

        monkeypatch.setattr(bounded_io, "read_receipt", unreadable)

    page, item = _task_page(owned_state, task_id)

    assert "workflow_run_id" not in item
    assert page.get("ownership_incomplete") or page["history_incomplete"]
    assert not page["counts_complete"]
    assert page["total_active_count"] is None


def test_related_parent_without_owner_receipt_has_explicit_standalone_null(owned_state):
    directory = owned_state / "tasks"
    task_store.write(directory, _task("parent-task"))
    task_store.write(directory, _task("child-task", spawned_by="parent-task", root_task_id="parent-task"))

    page = task_store.list_page(directory, task_ids=["child-task"])
    parent = next(item for item in page["related_headers"] if item["task_id"] == "parent-task")

    assert "workflow_run_id" in parent
    assert parent["workflow_run_id"] is None


def test_unreadable_related_parent_receipt_hides_owner_and_invalidates_counts(owned_state):
    directory = owned_state / "tasks"
    task_store.write(directory, _task("parent-task"))
    task_store.write(directory, _task("child-task", finished_at=None, spawned_by="parent-task", root_task_id="parent-task"))
    receipt = owned_state / "workflow-owners" / "parent-task.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text("{malformed", encoding="utf-8")

    page = task_store.list_page(directory, task_ids=["child-task"])
    parent = next(item for item in page["related_headers"] if item["task_id"] == "parent-task")

    assert "workflow_run_id" not in parent
    assert page["ownership_incomplete"]
    assert page["history_incomplete"]
    assert not page["counts_complete"]
    assert page["total_active_count"] is None


def test_bounded_header_preserves_lifecycle_identity_and_classification_metadata():
    required = {
        "task_id": "task-id",
        "finished_at": "2026-10-27T00:00:00+00:00",
        "resumed_at": 1_700_000_000.5,
        "workflow_run_id": "run-id",
        "workflow_id": "workflow-generation-id",
        "workflow_builder": False,
        "kind": "workflow",
        "settling": True,
        "parent_workflow_run_id": "parent-run-id",
        "root_workflow_run_id": "root-run-id",
        "parent_task_id": "parent-task-id",
        "chain_root_task_id": "chain-root-id",
        "root_task_id": "root-task-id",
    }
    source = required | {f"unprotected_{index}": "large value " * 500 for index in range(20)}

    bounded = catalog.bound_header(source)

    assert len(json.dumps(bounded, ensure_ascii=True).encode("utf-8")) <= 2_048
    assert {key: bounded[key] for key in required} == required
    assert catalog.bound_header({**source, "finished_at": 1_700_000_000.5})["finished_at"] == 1_700_000_000.5
    standalone = catalog.bound_header({**source, "workflow_run_id": None})
    assert "workflow_run_id" in standalone and standalone["workflow_run_id"] is None


def test_oversized_protected_workflow_identity_returns_unknown_direct_lookup_header():
    bounded = catalog.bound_header({
        "workflow_run_id": "oversized-run",
        "workflow_id": "generation-" + "x" * 3_000,
        "kind": "workflow",
        "status": "completed",
        "finished_at": 1_700_000_000.0,
        "settling": False,
    })

    assert len(json.dumps(bounded, ensure_ascii=True).encode("utf-8")) <= 2_048
    assert bounded["workflow_run_id"] == "oversized-run"
    assert bounded["needs_direct_lookup"]
    assert bounded["status"] == "unknown"


def _rewrite_projection_state(
    directory: Path,
    suffix: str,
    key: str,
    identifier: str,
    dropped: tuple[str, ...],
    layout,
    *,
    version: str = "1",
    overrides: dict | None = None,
) -> None:
    index = catalog.Catalog(directory, suffix, layout=layout)
    with index.connect() as db:
        row = db.execute("SELECT payload FROM entries WHERE id=?", (identifier,)).fetchone()
        assert row is not None
        payload = json.loads(row[0])
        for field in dropped:
            payload.pop(field, None)
        payload.update(overrides or {})
        db.execute("UPDATE entries SET payload=? WHERE id=?", (json.dumps(payload), identifier))
        db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (key, version))


def test_old_task_and_run_projection_versions_rebuild_required_metadata(workflow_store, owned_state):
    storage, _ = workflow_store
    layout = resolve_layout(root=owned_state)
    task_id = "projection-task"
    task_store.write(owned_state / "tasks", _task(task_id))
    task_store.list_page(owned_state / "tasks", task_ids=[task_id])
    _rewrite_projection_state(
        owned_state / "tasks", ".meta.json", "task_projection_version", task_id,
        ("finished_at",), layout,
    )

    task = task_store.list_page(owned_state / "tasks", task_ids=[task_id])["items"][0]
    assert task["finished_at"] == "2026-10-27T00:00:00+00:00"

    run = _create_run(workflow_store)
    storage.list_run_page(run_ids=[run["workflow_run_id"]])
    _rewrite_projection_state(
        storage.runs, ".json", "run_projection_version", run["workflow_run_id"],
        ("workflow_id",), layout, version="2", overrides={"finished_at": 1_700_000_000.0},
    )

    header = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
    assert header["finished_at"] is None
    assert header["workflow_id"] == workflow_references.workflow_identity(run["definition"])
    with catalog.Catalog(storage.runs, ".json", layout=layout).connect() as db:
        assert db.execute("SELECT value FROM state WHERE key='run_projection_version'").fetchone() == ("4",)


def test_v9_and_v10_workflow_catalogs_keep_independent_projection_freshness(workflow_store, monkeypatch):
    import sqlite3

    storage, repo = workflow_store
    run = storage.create_run(_definition(), "catalog compatibility", repo)
    clock = _Clock(1_700_000_000.0)
    monkeypatch.setattr(workflows, "time", clock)
    run = storage.update_run(run["workflow_run_id"], lambda row: row.update(status="completed"), "finish")
    old_path = storage.runs / ".listing.v9.sqlite3"
    current_path = catalog.catalog_path(storage.runs)
    original_header = workflows.WorkflowStore.run_header

    def legacy_header(self, value):
        header = original_header(self, value)
        header.pop("workflow_id", None)
        header.pop("finished_at", None)
        return header

    with monkeypatch.context() as old_engine:
        old_engine.setattr(catalog, "CATALOG_FILENAME", ".listing.v9.sqlite3")
        old_engine.setattr(catalog, "TASK_PROJECTION_VERSION", "1")
        old_engine.setattr(catalog, "RUN_PROJECTION_VERSION", "1")
        old_engine.setattr(workflows.WorkflowStore, "run_header", legacy_header)
        old_header_value = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
        assert "workflow_id" not in old_header_value
        assert "finished_at" not in old_header_value
        with sqlite3.connect(old_path) as db:
            assert db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone() == ("11",)
            assert db.execute("SELECT value FROM state WHERE key='run_projection_version'").fetchone() == ("1",)

    current_header = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
    assert current_header["workflow_id"] == run["definition"]["workflow_id"]
    assert current_header["finished_at"] == 1_700_000_000.0
    with sqlite3.connect(current_path) as db:
        assert db.execute("SELECT value FROM state WHERE key='run_projection_version'").fetchone() == ("4",)

    clock.value = 1_700_000_010.0
    updated = storage.update_run(run["workflow_run_id"], lambda row: row.update(summary="updated"), "bookkeeping")
    assert updated["finished_at"] == 1_700_000_000.0
    with monkeypatch.context() as old_engine:
        old_engine.setattr(catalog, "CATALOG_FILENAME", ".listing.v9.sqlite3")
        old_engine.setattr(catalog, "TASK_PROJECTION_VERSION", "1")
        old_engine.setattr(catalog, "RUN_PROJECTION_VERSION", "1")
        old_engine.setattr(workflows.WorkflowStore, "run_header", legacy_header)
        refreshed_old = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
        assert refreshed_old["updated_at"] == 1_700_000_010.0
        assert "finished_at" not in refreshed_old
    refreshed_new = storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"][0]
    assert refreshed_new["updated_at"] == 1_700_000_010.0
    assert refreshed_new["finished_at"] == 1_700_000_000.0

    (storage.runs / f"{run['workflow_run_id']}.json").unlink()
    with monkeypatch.context() as old_engine:
        old_engine.setattr(catalog, "CATALOG_FILENAME", ".listing.v9.sqlite3")
        old_engine.setattr(catalog, "TASK_PROJECTION_VERSION", "1")
        old_engine.setattr(catalog, "RUN_PROJECTION_VERSION", "1")
        old_engine.setattr(workflows.WorkflowStore, "run_header", legacy_header)
        assert storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"] == []
    assert storage.list_run_page(run_ids=[run["workflow_run_id"]])["items"] == []


def test_final_owner_enrichment_direct_lookup_sentinel_makes_task_page_incomplete(workflow_store, owned_state, monkeypatch):
    storage, repo = workflow_store
    run = storage.create_run(_definition(), "owner enrichment fixture", repo)
    task_id = "oversized-owner-task"
    task_store.write(owned_state / "tasks", _task(task_id))
    _write_receipt(owned_state, task_id, run["workflow_run_id"])
    bound = catalog.bound_header

    def over_budget_after_enrichment(header):
        bounded = bound(header)
        if header.get("task_id") == task_id and header.get("workflow_run_id") == run["workflow_run_id"]:
            return {
                "task_id": task_id,
                "workflow_run_id": run["workflow_run_id"],
                "status": "unknown",
                "needs_direct_lookup": True,
                "index_blocker": "header_metadata_budget",
            }
        return bounded

    monkeypatch.setattr(catalog, "bound_header", over_budget_after_enrichment)

    page = task_store.list_page(owned_state / "tasks", task_ids=[task_id])

    assert page["items"][0]["needs_direct_lookup"]
    assert page["items"][0]["status"] == "unknown"
    assert page["history_incomplete"]
    assert not page["counts_complete"]
    assert page["total_active_count"] is None


def test_canonical_metadata_page_fixtures_are_manifested_and_wire_valid():
    fixture_root = Path(__file__).parents[1] / "contracts" / "v1"
    manifest_bytes = (fixture_root / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    expected_digest = (fixture_root / "manifest.sha256").read_text().split()[0]

    assert hashlib.sha256(manifest_bytes).hexdigest() == expected_digest
    assert set(manifest["files"]) == set(manifest["cases"])
    expected = {
        "ctl-task-page-metadata.json": ("workflow_run_id",),
        "ctl-workflow-page-metadata.json": ("workflow_id", "finished_at"),
    }
    for filename, fields in expected.items():
        case = manifest["cases"][filename]
        assert case["format"] == CTL_FORMAT and case["valid"]
        document = json.loads((fixture_root / filename).read_text())
        validate_envelope(document, CTL_FORMAT)
        for field in fields:
            assert any(field in row for row in document["result"]["items"])
        assert hashlib.sha256((fixture_root / filename).read_bytes()).hexdigest() == manifest["files"][filename]

    task_fixture = json.loads((fixture_root / "ctl-task-page-metadata.json").read_text())
    task_items = task_fixture["result"]["items"]
    assert any("workflow_run_id" in row and row["workflow_run_id"] is None for row in task_items)
    assert any(isinstance(row.get("workflow_run_id"), str) and row["workflow_run_id"] for row in task_items)
    assert any(isinstance(row.get("finished_at"), str) for row in task_items)

    workflow_fixture = json.loads((fixture_root / "ctl-workflow-page-metadata.json").read_text())
    workflow_items = workflow_fixture["result"]["items"]
    assert any(isinstance(row.get("workflow_id"), str) and isinstance(row.get("finished_at"), (int, float)) for row in workflow_items)
    assert any("finished_at" not in row or row["finished_at"] is None for row in workflow_items)
    assert any(isinstance(row.get("resumed_at"), (int, float)) for row in workflow_items)
    assert any("resumed_at" not in row for row in workflow_items)
