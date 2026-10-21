"""Pure workflow-run envelope and authority qualification regressions."""

from __future__ import annotations

import copy
import json
import math
from pathlib import Path

import pytest
from mcp.shared.exceptions import MCPError

from harflow_mcp import workflow_run_documents as documents
from harflow_mcp import workflow_invocation
from harflow_mcp import workflows
from harflow_mcp import catalog
from harflow_mcp import followups, store as task_store
from harflow_mcp import server, state_ownership


def _definition() -> dict:
    return {
        "name": "run-document-fixture",
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {
                "id": "work",
                "type": "agent",
                "role": "task",
                "instructions": "Read the fixture",
                "agent": {"backend": "codex"},
            },
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


@pytest.fixture
def run_document(tmp_path, monkeypatch) -> dict:
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    definition = workflows.validate_definition(_definition())
    return store.create_run(definition, "fixture prompt", Path.cwd(), permission_policy="saved_node")


@pytest.fixture
def store_and_run(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    definition = workflows.validate_definition(_definition())
    run = store.create_run(definition, "fixture prompt", Path.cwd(), permission_policy="saved_node")
    return store, run


def test_encode_decode_round_trip_preserves_inner_run_and_stamps_envelope(run_document):
    run = copy.deepcopy(run_document)
    run["future_run_field"] = {"kept": [True, "as-authored"]}
    run["definition"]["future_definition_field"] = {"opaque": "snapshot"}

    encoded = documents.encode(run)

    assert encoded["product"] == "harflow"
    assert encoded["format"] == "harflow.workflow.run"
    assert encoded["v"] == 1
    assert encoded["run"] == run
    decoded = documents.decode(encoded, run["workflow_run_id"])
    assert decoded == run
    assert decoded is not run
    assert decoded["future_run_field"] == run["future_run_field"]


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value.pop("product"),
        lambda value: value.update(product="foreign"),
        lambda value: value.update(format="harflow.workflow.definition"),
        lambda value: value.update(v=True),
        lambda value: value.update(v=2),
        lambda value: value.update(product_version="2.0.0"),
        lambda value: value.update(product_version="1.0.0-alpha"),
        lambda value: value.update(run=[]),
    ],
)
def test_decode_rejects_foreign_or_malformed_envelopes_without_normalizing(run_document, mutator):
    document = documents.encode(run_document)
    mutator(document)
    before = copy.deepcopy(document)

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        documents.decode(document, run_document["workflow_run_id"])

    assert caught.value.code == "workflow_run_incompatible"
    assert document == before


def test_decode_requires_filename_identity_before_authority_use(run_document):
    document = documents.encode(run_document)

    with pytest.raises(documents.WorkflowRunDocumentError, match="run id"):
        documents.decode(document, "different-run-id")


@pytest.mark.parametrize(
    "field,value",
    [
        ("revision", True),
        ("sequence", 1.0),
        ("transitions", -1),
        ("created_at", math.nan),
        ("updated_at", math.inf),
        ("permission_policy", 1),
        ("activations", {}),
        ("sessions", []),
    ],
)
def test_decode_rejects_malformed_known_authority_fields(run_document, field, value):
    document = documents.encode(run_document)
    document["run"][field] = value

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        documents.decode(document, run_document["workflow_run_id"])

    assert caught.value.code == "workflow_run_incompatible"


def _activation_fixture(*, token=None):
    activation = {
        "id": "activation",
        "node_id": "work",
        "role": "node",
        "status": "completed",
        "created_at": 0.0,
        "tasks": [],
    }
    if token is not None:
        activation["token"] = token
    return activation


@pytest.mark.parametrize("value", [True, False])
def test_activation_optional_failure_accepts_only_real_booleans(run_document, value):
    run = copy.deepcopy(run_document)
    activation = _activation_fixture()
    activation["optional_failure"] = value
    run["activations"] = [activation]

    assert documents.decode(documents.encode(run), run["workflow_run_id"])["activations"][0]["optional_failure"] is value


@pytest.mark.parametrize("value", ["false", 0, 1, {}, []])
def test_activation_optional_failure_rejects_non_boolean_without_mutation(run_document, value):
    run = copy.deepcopy(run_document)
    run["activations"] = [_activation_fixture()]
    document = documents.encode(run)
    document["run"]["activations"][0]["optional_failure"] = value
    before = copy.deepcopy(document)

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        documents.decode(document, run["workflow_run_id"])

    assert caught.value.code == "workflow_run_incompatible"
    assert document == before


@pytest.mark.parametrize("field", ["assigned_task_ids", "resolved_execution_refs", "input_result_refs"])
@pytest.mark.parametrize("value", ["one", ["ok", 1], [None], {}, [True]])
def test_activation_reference_fields_require_string_arrays(run_document, field, value):
    run = copy.deepcopy(run_document)
    run["activations"] = [_activation_fixture()]
    document = documents.encode(run)
    document["run"]["activations"][0][field] = value
    before = copy.deepcopy(document)

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        documents.decode(document, run["workflow_run_id"])

    assert caught.value.code == "workflow_run_incompatible"
    assert document == before


@pytest.mark.parametrize(
    "result",
    [
        {"status": "failed", "failure_kind": "permission"},
        {"status": "failed", "result": {"failure_kind": "permission"}, "evidence": []},
        {"status": "failed", "optional_failure": "observed failed", "failure_evidence": {"status": "failed"}},
    ],
)
def test_token_optional_failure_join_and_known_result_variants_are_preserved(run_document, result):
    run = copy.deepcopy(run_document)
    token = {
        "id": "token",
        "optional_failure_join": "join",
        "input_result_refs": ["source"],
        "result": result,
    }
    run["activations"] = [_activation_fixture(token=token)]
    run["authored_evidence"] = {"opaque": "preserve"}

    decoded = documents.decode(documents.encode(run), run["workflow_run_id"])

    assert decoded["activations"][0]["token"] == token
    assert decoded["authored_evidence"] == run["authored_evidence"]


@pytest.mark.parametrize("field,value", [("optional_failure_join", 1), ("optional_failure_join", False), ("optional_failure_join", {}), ("result", "failed")])
def test_token_optional_failure_fields_refuse_malformed_values_before_publication(store_and_run, field, value):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    document = json.loads(path.read_text())
    document["run"]["activations"] = [_activation_fixture(token={"id": "token", field: value})]
    path.write_text(json.dumps(document), encoding="utf-8")
    before = path.read_bytes()

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"])

    assert caught.value.code == "workflow_run_incompatible"
    assert path.read_bytes() == before


def test_decode_rejects_raw_legacy_run_document(run_document):
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        documents.decode(run_document, run_document["workflow_run_id"])
    assert caught.value.code == "workflow_run_incompatible"


def test_store_persists_envelope_but_returns_inner_run(store_and_run):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    raw = json.loads(path.read_text())

    assert raw["format"] == documents.RUN_FORMAT
    assert raw["run"] == run
    assert "product" not in store.get_run(run["workflow_run_id"])


def test_store_refuses_raw_legacy_run_without_mutating_bytes(store_and_run):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    path.write_text(json.dumps(run), encoding="utf-8")
    before = path.read_bytes()

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"])

    assert caught.value.code == "workflow_run_incompatible"
    assert path.read_bytes() == before


def test_child_writer_publishes_qualified_envelope(store_and_run):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    path.unlink()

    workflow_invocation.write_child_run(store, run)

    raw = json.loads(path.read_text())
    assert raw["format"] == documents.RUN_FORMAT
    assert raw["run"] == run
    assert store.get_run(run["workflow_run_id"])["workflow_run_id"] == run["workflow_run_id"]


def test_child_writer_refuses_invalid_run_before_creating_temp_file(store_and_run):
    store, run = store_and_run
    run = copy.deepcopy(run)
    run["revision"] = True
    path = store.runs / f"{run['workflow_run_id']}.json"
    path.unlink()

    with pytest.raises(documents.WorkflowRunDocumentError):
        workflow_invocation.write_child_run(store, run)

    assert not list(store.runs.glob(".*.tmp"))
    assert not path.exists()


def test_list_runs_fails_closed_on_unqualified_member(store_and_run):
    store, run = store_and_run
    bad = store.runs / "unqualified.json"
    bad.write_text(json.dumps(run), encoding="utf-8")

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.list_runs(strict=True)

    assert caught.value.code == "workflow_run_incompatible"


def test_decode_preserves_authored_builder_and_snapshot_values(run_document):
    run = copy.deepcopy(run_document)
    run.update(
        builder_draft={"nodes": [{"id": "incomplete", "type": "agent"}]},
        generated_definition={"raw": ["preview", 1]},
        editing_source={"name": "run-document-fixture", "revision": 3, "saved_definition": None},
        goal_history=[{"revision": 1, "goal": "Keep this exact text", "created_at": 1.25}],
    )
    document = documents.encode(run)
    decoded = documents.decode(document, run["workflow_run_id"])

    assert decoded["builder_draft"] == run["builder_draft"]
    assert decoded["generated_definition"] == run["generated_definition"]
    assert decoded["editing_source"] == run["editing_source"]
    assert decoded["goal_history"] == run["goal_history"]


def test_document_error_exposes_typed_diagnostic(run_document):
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        documents.decode({}, run_document["workflow_run_id"])
    error = caught.value
    assert isinstance(error, Exception)
    assert error.run_id == run_document["workflow_run_id"]
    assert error.as_dict()["code"] == "workflow_run_incompatible"


@pytest.mark.asyncio
async def test_mcp_boundary_maps_only_published_state_ownership_errors():
    @server._task_document_errors
    async def raise_ownership_error(published):
        error = state_ownership.StateOwnershipError("ownership outcome")
        error.published = published
        error.code = "state_incompatible"
        error.workflow_run_id = "workflow-run"
        raise error

    with pytest.raises(state_ownership.StateOwnershipError):
        await raise_ownership_error(False)

    with pytest.raises(MCPError) as caught:
        await raise_ownership_error(True)
    assert caught.value.data == {
        "code": "state_incompatible",
        "workflow_run_id": "workflow-run",
        "published": True,
        "reason": "ownership outcome",
    }


def test_create_run_reports_postpublication_index_failure(store_and_run, monkeypatch):
    store, existing = store_and_run
    monkeypatch.setattr(store, "_index_run", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("index unavailable")))

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.create_run(_definition(), "published", Path.cwd(), permission_policy="saved_node")

    error = caught.value
    assert error.published is True and error.code == "workflow_run_unavailable"
    published = [p for p in store.runs.glob("*.json") if p.name != f"{existing['workflow_run_id']}.json"]
    assert published and documents.decode(json.loads(published[0].read_text()), published[0].stem)["status"] == "starting"


def test_update_rejection_is_prepublication_and_preserves_bytes(store_and_run):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    before = path.read_bytes()

    with pytest.raises(documents.WorkflowRunDocumentError):
        store.update_run(run["workflow_run_id"], lambda value: value.update(revision=True), "invalid")

    assert path.read_bytes() == before


def test_update_rejects_attempt_without_task_identity_before_publication(store_and_run):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    before = {p.relative_to(store.root): p.read_bytes() for p in store.root.rglob("*") if p.is_file() and ".workflow-" not in p.name}

    def remove_attempt_identity(value):
        value["activations"] = [{
            "id": "execution", "node_id": "work", "role": "node", "status": "running",
            "created_at": 0.0, "tasks": [{"status": "reserved"}],
        }]

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], remove_attempt_identity, "edit")

    assert caught.value.published is False
    after = {p.relative_to(store.root): p.read_bytes() for p in store.root.rglob("*") if p.is_file() and ".workflow-" not in p.name}
    assert after == before


def test_update_reports_postpublication_index_failure(store_and_run, monkeypatch):
    store, run = store_and_run
    monkeypatch.setattr(store, "_index_run", lambda *args, **kwargs: (_ for _ in ()).throw(OSError("index unavailable")))

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(status="completed"), "settled")

    assert caught.value.published is True
    assert store.get_run(run["workflow_run_id"])["status"] == "completed"


def test_update_receipt_failure_reports_landed_run_and_recovery_evidence(store_and_run, monkeypatch):
    store, run = store_and_run
    real_write = workflows._write

    def fail_owner_receipt(path, value, **kwargs):
        if path.parent == store.owners:
            raise OSError("owner receipt unavailable")
        return real_write(path, value, **kwargs)

    monkeypatch.setattr(workflows, "_write", fail_owner_receipt)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(
            status="running", activations=[{"id": "execution", "node_id": "work", "role": "node",
            "status": "running", "created_at": 0.0, "tasks": [{"task_id": "attempt", "status": "reserved"}]}]), "edit")

    assert caught.value.published is True
    landed = store.get_run(run["workflow_run_id"])
    assert landed["status"] == "running"
    assert not (store.owners / "attempt.json").exists()
    monkeypatch.setattr(workflows, "_write", real_write)
    assert store.reconcile_run(run["workflow_run_id"])["workflow_run_id"] == run["workflow_run_id"]


def test_update_journal_fsync_failure_reports_landed_run_and_recovery_evidence(store_and_run, monkeypatch):
    store, run = store_and_run
    real_fsync = workflows.os.fsync
    calls = 0

    def fail_journal_fsync(fd):
        nonlocal calls
        calls += 1
        if calls == 5:
            raise OSError("journal fsync unavailable")
        return real_fsync(fd)

    monkeypatch.setattr(workflows.os, "fsync", fail_journal_fsync)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(status="completed", activations=[{"id": "execution", "node_id": "work", "role": "node", "status": "completed", "created_at": 0.0, "tasks": [{"task_id": "attempt", "status": "completed"}]}]), "edit")

    assert caught.value.published is True
    assert store.get_run(run["workflow_run_id"])["status"] == "completed"
    assert store.reconcile_run(run["workflow_run_id"])["workflow_run_id"] == run["workflow_run_id"]


def test_update_journal_open_failure_reports_landed_run_and_recovery_evidence(store_and_run, monkeypatch):
    store, run = store_and_run
    journal = store.runs / f"{run['workflow_run_id']}.jsonl"
    before = journal.read_bytes() if journal.exists() else b""
    original_open = Path.open

    def refuse_journal(path, *args, **kwargs):
        mode = kwargs.get("mode", args[0] if args else "r")
        if path == journal and "a" in mode:
            raise OSError("journal open unavailable")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", refuse_journal)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(
            status="running", activations=[{"id": "execution", "node_id": "work", "role": "node",
            "status": "running", "created_at": 0.0, "tasks": [{"task_id": "journal-attempt", "status": "reserved"}]}]), "edit")

    assert caught.value.published is True
    assert store.get_run(run["workflow_run_id"])["status"] == "running"
    receipt = json.loads((store.owners / "journal-attempt.json").read_text())
    assert receipt["workflow_run_id"] == run["workflow_run_id"]
    assert (not journal.exists()) if not before else journal.read_bytes() == before


def test_update_refreshes_valid_same_root_source_before_mutation(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original = store._read_run
    calls = 0

    def read(run_id, **kwargs):
        nonlocal calls
        result = original(run_id, **kwargs)
        calls += 1
        if calls == 1:
            replacement = copy.deepcopy(result[0])
            replacement["name"] = "replaced"
            path.write_text(json.dumps(documents.encode(replacement)))
        return result

    monkeypatch.setattr(store, "_read_run", read)
    updated = store.update_run(run["workflow_run_id"], lambda value: value.update(name="mutated"), "edit")
    assert calls >= 2
    assert updated["name"] == "mutated"
    assert store.get_run(run["workflow_run_id"])["name"] == "mutated"


def test_update_rejects_root_rebinding_before_mutator(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original = store._read_run
    calls = 0

    def read(run_id, **kwargs):
        nonlocal calls
        result = original(run_id, **kwargs)
        calls += 1
        if calls == 1:
            replacement = copy.deepcopy(result[0])
            replacement["parent_link"] = {"root_workflow_run_id": "foreign-root"}
            path.write_text(json.dumps(documents.encode(replacement)))
        return result

    monkeypatch.setattr(store, "_read_run", read)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(name="mutated"), "edit")
    assert caught.value.code == "workflow_run_unavailable"
    assert store.get_run(run["workflow_run_id"])["name"] == "run-document-fixture"


def test_update_rejects_unqualified_source_before_mutator(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original = store._read_run
    calls = 0

    def read(run_id, **kwargs):
        nonlocal calls
        result = original(run_id, **kwargs)
        calls += 1
        if calls == 1:
            path.write_text(json.dumps(result[0]))
        return result

    monkeypatch.setattr(store, "_read_run", read)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(name="mutated"), "edit")
    assert caught.value.code == "workflow_run_incompatible"
    assert json.loads(path.read_text()) == run


def test_update_rejects_source_replacement_after_locked_read(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original = store._record_notification_point

    def record(value, *args, **kwargs):
        original(value, *args, **kwargs)
        replacement = copy.deepcopy(value)
        replacement["name"] = "substituted"
        path.write_text(json.dumps(documents.encode(replacement)))

    monkeypatch.setattr(store, "_record_notification_point", record)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(run["workflow_run_id"], lambda value: value.update(name="mutated"), "edit")
    assert caught.value.code == "workflow_run_unavailable"
    assert store.get_run(run["workflow_run_id"])["name"] == "substituted"


def test_read_run_requalifies_valid_replacement_before_open(store_and_run, monkeypatch):
    """A replacement observed before open is the current source, not a stale race."""
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_open = workflows.open_descriptor
    replacement = copy.deepcopy(run)
    replacement["name"] = "freshly opened"
    replaced = False

    def replace_before_open(candidate, *args, **kwargs):
        nonlocal replaced
        if Path(candidate) == path and not replaced:
            replaced = True
            path.write_text(json.dumps(documents.encode(replacement)))
        return original_open(candidate, *args, **kwargs)

    monkeypatch.setattr(workflows, "open_descriptor", replace_before_open)
    assert store.get_run(run["workflow_run_id"], refresh_atomic=True)["name"] == "freshly opened"


def test_refresh_atomic_refuses_a_second_replacement(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_fdopen = workflows.os.fdopen
    opens = 0
    reads = 0

    class ReplacingStream:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            self.stream.__enter__()
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal reads
            data = self.stream.read(*args, **kwargs)
            reads += 1
            replacement = copy.deepcopy(run)
            replacement["name"] = f"replacement-{reads}"
            tmp = path.with_suffix(f".{reads}.tmp")
            tmp.write_text(json.dumps(documents.encode(replacement)))
            tmp.replace(path)
            return data

    def fdopen(fd, *args, **kwargs):
        return ReplacingStream(original_fdopen(fd, *args, **kwargs))

    monkeypatch.setattr(workflows.os, "fdopen", fdopen)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"], refresh_atomic=True)
    assert caught.value.code == "workflow_run_unavailable"
    assert reads == 2


def test_refresh_atomic_charges_budget_for_superseded_read(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_open = workflows.open_descriptor
    original_fdopen = workflows.os.fdopen
    replaced = False

    class ReplacingStream:
        def __init__(self, stream):
            self.stream = stream
        def __enter__(self):
            self.stream.__enter__()
            return self
        def __exit__(self, *args):
            return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                replacement = copy.deepcopy(run)
                replacement["name"] = "budget replacement"
                tmp = path.with_suffix('.budget.tmp')
                tmp.write_text(json.dumps(documents.encode(replacement)))
                tmp.replace(path)
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: ReplacingStream(original_fdopen(fd, *args, **kwargs)))
    budget = catalog.MetadataBudget(metadata_limit=10 * 1024 * 1024)
    result = store.get_run(run["workflow_run_id"], metadata_budget=budget, refresh_atomic=True)
    assert result["name"] == "budget replacement"
    assert budget.metadata_decodes == 2
    assert budget.metadata_bytes > 0


def test_refresh_atomic_discards_invalid_superseded_bytes_before_decode(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_fdopen = workflows.os.fdopen
    original_decode = documents.decode
    decoded = []
    replaced = False

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                replacement = copy.deepcopy(run)
                replacement["name"] = "valid current"
                tmp = path.with_suffix('.valid.tmp')
                tmp.write_text(json.dumps(documents.encode(replacement)))
                tmp.replace(path)
                return b"{invalid superseded json"
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))

    def spy_decode(document, run_id):
        decoded.append(document)
        return original_decode(document, run_id)

    monkeypatch.setattr(documents, "decode", spy_decode)
    assert store.get_run(run["workflow_run_id"], refresh_atomic=True)["name"] == "valid current"
    assert decoded and all(document.get("run", {}).get("name") != "invalid superseded json" for document in decoded)


def test_refresh_atomic_budget_exhaustion_does_not_reset_after_discarded_read(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_fdopen = workflows.os.fdopen
    replaced = False

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                replacement = copy.deepcopy(run)
                replacement["name"] = "budget current"
                tmp = path.with_suffix('.budget-cap.tmp')
                tmp.write_text(json.dumps(documents.encode(replacement)))
                tmp.replace(path)
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    budget = catalog.MetadataBudget(metadata_limit=len(path.read_bytes()) + 1)
    with pytest.raises(__import__('harflow_mcp.bounded_io', fromlist=['ReadLimit']).ReadLimit):
        store.get_run(run["workflow_run_id"], metadata_budget=budget, refresh_atomic=True)
    assert budget.metadata_decodes == 1
    assert budget.metadata_bytes > 0


def test_refresh_atomic_refuses_same_inode_change_after_final_descriptor_check(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_source = store._run_source
    calls = 0

    def mutate_before_path_observation(run_id):
        nonlocal calls
        calls += 1
        if calls == 1:
            replacement = copy.deepcopy(run)
            replacement["name"] = "same inode changed after fstat"
            path.write_text(json.dumps(documents.encode(replacement)))
        return original_source(run_id)

    monkeypatch.setattr(store, "_run_source", mutate_before_path_observation)
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"], refresh_atomic=True)
    assert caught.value.code == "workflow_run_unavailable"
    assert calls == 1


@pytest.mark.parametrize("replacement", [
    b"{not-json",
    {"product": "foreign", "format": documents.RUN_FORMAT, "v": 1, "run": {}},
    "wrong-id",
    "malformed-authority",
])
def test_refresh_atomic_never_returns_superseded_run_for_invalid_current_source(store_and_run, monkeypatch, replacement):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_fdopen = workflows.os.fdopen
    replaced = False

    if isinstance(replacement, dict):
        document = copy.deepcopy(replacement)
        if replacement.get("product") == "foreign":
            payload = json.dumps(document).encode()
        else:
            document = documents.encode(run)
            if replacement == "wrong-id":
                document["run"]["workflow_run_id"] = "different-run"
            else:
                document["run"]["permission_policy"] = 1
            payload = json.dumps(document).encode()
    elif replacement == "wrong-id":
        document = documents.encode(run)
        document["run"]["workflow_run_id"] = "different-run"
        payload = json.dumps(document).encode()
    elif replacement == "malformed-authority":
        document = documents.encode(run)
        document["run"]["permission_policy"] = 1
        payload = json.dumps(document).encode()
    else:
        payload = replacement

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                temporary = path.with_suffix(".invalid-current.tmp")
                temporary.write_bytes(payload)
                temporary.replace(path)
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"], refresh_atomic=True)
    assert caught.value.code == "workflow_run_incompatible"


@pytest.mark.parametrize("kind", ["missing", "symlink", "directory"])
def test_refresh_atomic_reports_invalid_current_replacement_as_unavailable(store_and_run, monkeypatch, tmp_path, kind):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_fdopen = workflows.os.fdopen
    replacement = copy.deepcopy(run)
    replacement["name"] = "replacement that must never be decoded"
    target = tmp_path / "replacement.json"
    target.write_text(json.dumps(documents.encode(replacement)))
    replaced = False

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                path.unlink()
                if kind == "symlink":
                    path.symlink_to(target)
                elif kind == "directory":
                    path.mkdir()
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"], refresh_atomic=True)
    assert caught.value.code == "workflow_run_unavailable"


def test_refresh_atomic_decode_cap_is_not_reset_after_discarded_read(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_fdopen = workflows.os.fdopen
    replaced = False

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                replacement = copy.deepcopy(run)
                replacement["name"] = "second-read"
                temporary = path.with_suffix(".decode-cap.tmp")
                temporary.write_text(json.dumps(documents.encode(replacement)))
                temporary.replace(path)
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    budget = catalog.MetadataBudget(metadata_limit=10 * 1024 * 1024, metadata_decodes=99)
    with pytest.raises(__import__("harflow_mcp.bounded_io", fromlist=["ReadLimit"]).ReadLimit):
        store.get_run(run["workflow_run_id"], metadata_budget=budget, refresh_atomic=True)
    assert budget.metadata_decodes == 100


def test_refresh_atomic_second_open_missing_is_typed_unavailable(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_open = workflows.open_descriptor
    original_fdopen = workflows.os.fdopen
    opens = 0
    replaced = False

    def open_descriptor(candidate, *args, **kwargs):
        nonlocal opens
        opens += 1
        if opens == 2:
            path.unlink()
        return original_open(candidate, *args, **kwargs)

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                fresh = copy.deepcopy(run)
                fresh["name"] = "replacement before missing second open"
                temporary = path.with_suffix(".second-open-missing.tmp")
                temporary.write_text(json.dumps(documents.encode(fresh)))
                temporary.replace(path)
            return data

    monkeypatch.setattr(workflows, "open_descriptor", open_descriptor)
    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"], refresh_atomic=True)
    assert caught.value.code == "workflow_run_unavailable"
    assert opens == 2


def test_locked_mutator_tree_running_keeps_strict_default(store_and_run, monkeypatch):
    store, root = store_and_run
    child = store.create_run(root["definition"], "child", Path.cwd())
    child_id = child["workflow_run_id"]
    root_id = root["workflow_run_id"]
    store.update_run(child_id, lambda value: value.update(status="running", parent_link={"root_workflow_run_id": root_id}), "attach")
    tree = workflow_invocation.WorkflowTree(store)
    path = store.runs / f"{root_id}.json"
    original_open = workflows.open_descriptor
    replaced = False

    def replace_root_after_open(candidate, *args, **kwargs):
        nonlocal replaced
        descriptor = original_open(candidate, *args, **kwargs)
        if Path(candidate) == path and not replaced:
            replaced = True
            fresh = copy.deepcopy(root)
            fresh["name"] = "root replaced during locked authority check"
            temporary = path.with_suffix(".locked-tree-refresh.tmp")
            temporary.write_text(json.dumps(documents.encode(fresh)))
            temporary.replace(path)
        return descriptor

    monkeypatch.setattr(workflows, "open_descriptor", replace_root_after_open)

    def mutate(value):
        if not tree.tree_running(value):
            raise documents.WorkflowRunDocumentError(child_id, "tree authority became unavailable", code="workflow_run_unavailable")
        value["name"] = "must not land"

    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.update_run(child_id, mutate, "locked_tree_authority")
    assert caught.value.code == "workflow_run_unavailable"
    assert store.get_run(child_id)["name"] == child["name"]


def test_get_run_placeholder_refresh_binds_catalog_fill_to_final_source(store_and_run, monkeypatch):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    listing = catalog.Catalog(store.runs, ".json", layout=store.state_layout)
    listing.record({"workflow_run_id": run["workflow_run_id"], "status": "unknown", "needs_direct_lookup": True}, 0.0, True, run["workflow_run_id"])
    original_fdopen = workflows.os.fdopen
    replaced = False
    recorded = []
    original_record = catalog.Catalog.record

    def capture(self, header, *args, **kwargs):
        recorded.append(dict(header))
        return original_record(self, header, *args, **kwargs)

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal replaced
            data = self.stream.read(*args, **kwargs)
            if not replaced:
                replaced = True
                replacement = copy.deepcopy(run)
                replacement["name"] = "final source"
                temporary = path.with_suffix(".placeholder-refresh.tmp")
                temporary.write_text(json.dumps(documents.encode(replacement)))
                temporary.replace(path)
            return data

    monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    monkeypatch.setattr(catalog.Catalog, "record", capture)
    result = store.get_run(run["workflow_run_id"], refresh_atomic=True)
    assert result["name"] == "final source"
    assert recorded and tuple(recorded[-1]["_source_identity"]) == catalog.source_identity(path.stat())


@pytest.mark.parametrize("mode", ["replace", "disappear", "symlink", "inplace"])
def test_read_run_rejects_source_change_after_open(store_and_run, monkeypatch, tmp_path, mode):
    store, run = store_and_run
    path = store.runs / f"{run['workflow_run_id']}.json"
    original_open = workflows.open_descriptor
    replacement = copy.deepcopy(run)
    replacement["name"] = "post-open substitute"
    substitute = tmp_path / "substitute.json"
    substitute.write_text(json.dumps(documents.encode(replacement)))
    mutated = False

    def mutate_after_open(candidate, *args, **kwargs):
        nonlocal mutated
        descriptor = original_open(candidate, *args, **kwargs)
        if Path(candidate) == path and not mutated:
            mutated = True
            if mode == "replace":
                replacement_path = tmp_path / "replacement.json"
                replacement_path.write_text(json.dumps(documents.encode(replacement)))
                replacement_path.replace(path)
            elif mode == "disappear":
                path.unlink()
            elif mode == "symlink":
                path.unlink()
                path.symlink_to(substitute)
        return descriptor

    monkeypatch.setattr(workflows, "open_descriptor", mutate_after_open)
    if mode == "inplace":
        original_fdopen = workflows.os.fdopen

        class InplaceStream:
            def __init__(self, stream):
                self.stream = stream
            def __enter__(self):
                self.stream.__enter__()
                return self
            def __exit__(self, *args):
                return self.stream.__exit__(*args)
            def read(self, *args, **kwargs):
                data = self.stream.read(*args, **kwargs)
                inplace = copy.deepcopy(replacement)
                inplace["name"] = "post-open in-place content with a deliberately changed size"
                path.write_text(json.dumps(documents.encode(inplace)))
                return data

        monkeypatch.setattr(workflows.os, "fdopen", lambda fd, *args, **kwargs: InplaceStream(original_fdopen(fd, *args, **kwargs)))
    with pytest.raises(documents.WorkflowRunDocumentError) as caught:
        store.get_run(run["workflow_run_id"])
    assert caught.value.code == "workflow_run_unavailable"


def test_read_run_accepts_metadata_budget_without_byte_limit(store_and_run):
    store, run = store_and_run
    budget = catalog.MetadataBudget(metadata_limit=10 * 1024 * 1024)
    assert store.get_run(run["workflow_run_id"], metadata_budget=budget)["workflow_run_id"] == run["workflow_run_id"]
    assert budget.metadata_decodes == 1 and budget.metadata_bytes > 0


def test_task_snapshot_preserves_followup_object_and_nullable_unavailable_count(tmp_path, monkeypatch):
    from harflow_mcp.state_layout import resolve_layout
    root = tmp_path / "state"
    log_dir = root / "tasks"
    record = task_store.TaskRecord(
        task_id="snapshot-task", backend="codex", session_id="session",
        repo_path=str(tmp_path), started_at="2026-10-21T00:00:00Z",
        status="completed", exit_code=0, finished_at="2026-10-21T00:01:00Z",
        freedom="read_only", output_drained=True,
    )
    with state_ownership.state_session(resolve_layout(root=root)):
        log_dir.mkdir(parents=True)
        assert task_store.write_landed(log_dir, record)

        available = task_store.snapshot(log_dir, task_store.read(log_dir, record.task_id))
        assert available["queued_followups"] == {"state": "available", "items": [], "omitted_final": 0}
        assert available["queued_followup_count"] == 0

        monkeypatch.setattr(
            followups, "view",
            lambda *_args, **_kwargs: {"state": "unavailable", "items": [], "omitted_final": 0},
        )
        unavailable = task_store.snapshot(log_dir, task_store.read(log_dir, record.task_id))
        assert unavailable["queued_followups"]["state"] == "unavailable"
        assert unavailable["queued_followup_count"] is None
