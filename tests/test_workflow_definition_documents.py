"""Persisted workflow-definition envelope and refusal contract."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from harflow_mcp import workflow_definition_documents as documents
from harflow_mcp import workflows


def definition(name: str = "fixture") -> dict:
    return {
        "name": name,
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {
                "id": "work",
                "type": "agent",
                "role": "task",
                "instructions": "Do the work",
                "agent": {"backend": "codex"},
            },
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value), encoding="utf-8")


def test_encode_decode_round_trip_keeps_authored_payload_and_stamps_envelope():
    payload = definition()
    encoded = documents.encode(payload)

    assert encoded["product"] == "harflow"
    assert encoded["format"] == "harflow.workflow.definition"
    assert encoded["v"] == 1
    assert encoded["definition"] == payload
    assert documents.decode(encoded, "fixture") == payload


@pytest.mark.parametrize(
    "version",
    ["0.0.7", "0.0.1+build.7", "0.0.1"],
)
def test_decode_accepts_compatible_stable_versions(version):
    document = documents.encode(definition())
    document["product_version"] = version
    assert documents.decode(document, "fixture")["name"] == "fixture"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value.pop("product"),
        lambda value: value.update(product="other"),
        lambda value: value.update(format="harflow.task"),
        lambda value: value.update(v=True),
        lambda value: value.update(product_version="0.1.0"),
        lambda value: value.update(product_version="1.0.0"),
        lambda value: value.update(product_version="0.0.1-beta.1"),
        lambda value: value.update(definition=[]),
        lambda value: value.update(definition={"name": 1}),
    ],
)
def test_decode_refuses_unqualified_or_malformed_documents_without_normalizing(mutator):
    document = documents.encode(definition())
    mutator(document)
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        documents.decode(document, "fixture")
    assert caught.value.code == "workflow_definition_incompatible"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value["orchestrator"].update(backend=1),
        lambda value: value["orchestrator"].update(fallbacks="not-a-list"),
        lambda value: value["orchestrator"].update(fallbacks=["not-an-object"]),
        lambda value: value["orchestrator"].update(fallbacks=[{"backend": 1}]),
        lambda value: value.__setitem__("nodes", "not-a-list"),
        lambda value: value["nodes"].__setitem__(0, "not-an-object"),
        lambda value: value["nodes"][1].update(agent={"backend": 1}),
        lambda value: value["nodes"][1].update(workflow_ref={"workflow_id": 1}),
        lambda value: value.__setitem__("connections", ["not-an-object"]),
        lambda value: value["connections"][0].update(max_retries=True),
        lambda value: value.__setitem__("revision", True),
        lambda value: value.__setitem__("max_parallel", True),
        lambda value: value["nodes"][1].update(max_attempts=True),
        lambda value: value.__setitem__("updated_at", float("nan")),
        lambda value: value["nodes"][1].update(position={"x": float("inf"), "y": 0}),
    ],
)
def test_decode_rejects_malformed_known_nested_types_and_nonfinite_numbers(mutator):
    malformed = documents.encode(definition())
    mutator(malformed["definition"])
    with pytest.raises(documents.WorkflowDefinitionDocumentError):
        documents.decode(malformed, "fixture")


def test_decode_accepts_additive_unknown_fields_but_rejects_known_nested_type_errors():
    document = documents.encode(definition())
    document["future_field"] = {"kept": True}
    document["definition"]["future_definition_field"] = "preserved"
    document["definition"]["orchestrator"]["future_orchestrator_field"] = {"kept": True}
    document["definition"]["nodes"][1]["future_node_field"] = ["kept"]
    document["definition"]["connections"][0]["future_connection_field"] = 7
    assert documents.decode(document, "fixture")["future_definition_field"] == "preserved"
    assert documents.decode(document, "fixture")["orchestrator"]["future_orchestrator_field"] == {"kept": True}


def test_decode_compares_normalized_inner_name_to_requested_source_name():
    document = documents.encode(definition("My Workflow"))
    assert documents.decode(document, "My Workflow")["name"] == "My Workflow"
    with pytest.raises(documents.WorkflowDefinitionDocumentError, match="name"):
        documents.decode(document, "Other Workflow")


def test_decode_refuses_recursively_nested_json_values_as_typed_incompatible():
    document = documents.encode(definition())
    nested = value = {}
    for _ in range(2500):
        value["next"] = {}
        value = value["next"]
    document["definition"]["future_nested_field"] = nested
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        documents.decode(document, "fixture")
    assert caught.value.code == "workflow_definition_incompatible"


def test_store_writes_envelope_and_returns_inner_projection(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    saved = store.save("fixture", definition())

    raw = json.loads((store.definitions / "fixture.json").read_text())
    assert raw["format"] == "harflow.workflow.definition"
    assert raw["definition"]["name"] == "fixture"
    assert "product" not in saved
    assert saved["name"] == "fixture"
    assert store.get("fixture") == saved
    assert store.list() == [saved]


def test_store_refuses_raw_legacy_definition_without_mutating_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    path = store.definitions / "fixture.json"
    raw = definition()
    write_json(path, raw)
    before = path.read_bytes()

    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.get("fixture")
    assert caught.value.code == "workflow_definition_incompatible"
    assert path.read_bytes() == before


def test_store_list_refuses_an_unqualified_member_instead_of_returning_partial_results(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    store.save("good", definition("good"))
    write_json(store.definitions / "bad.json", definition("bad"))

    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.list()
    assert caught.value.code == "workflow_definition_incompatible"


@pytest.mark.parametrize("replacement", ["foreign", "qualified-different-id", "symlink"])
def test_save_refuses_destination_that_appears_after_verified_absence(tmp_path, monkeypatch, replacement):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    target = store.definitions / "fixture.json"
    outside = tmp_path / "outside.json"
    write_json(outside, documents.encode(definition("outside")))

    def guard(_tree):
        if replacement == "foreign":
            write_json(target, {"name": "fixture"})
        elif replacement == "qualified-different-id":
            write_json(target, documents.encode(definition("different")))
        else:
            target.symlink_to(outside)

    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.save("fixture", definition(), authority_guard=guard)
    assert caught.value.code == "workflow_definition_unavailable"
    assert target.is_symlink() if replacement == "symlink" else target.exists()


@pytest.mark.parametrize("action", ["replace", "disappear"])
def test_save_rechecks_existing_source_identity_before_replacement(tmp_path, monkeypatch, action):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    store.save("fixture", definition())
    target = store.definitions / "fixture.json"
    replacement = tmp_path / "replacement.json"

    def guard(_tree):
        if action == "disappear":
            target.unlink()
        else:
            write_json(replacement, documents.encode(definition("replacement")))
            os.replace(replacement, target)

    before = target.read_bytes()
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.save("fixture", definition(), expected_revision=1, authority_guard=guard)
    assert caught.value.code == "workflow_definition_unavailable"
    if action == "replace":
        assert json.loads(target.read_text())["definition"]["name"] == "replacement"
    else:
        assert not target.exists()
    assert before != target.read_bytes() if action == "replace" else True


@pytest.mark.parametrize("bad_document", [definition("broken"), {"product": "harflow"}])
def test_delete_scans_all_referrers_before_unlinking_target(tmp_path, monkeypatch, bad_document):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    store.save("target", definition("target"))
    bad = store.definitions / "unrelated.json"
    write_json(bad, bad_document)
    target = store.definitions / "target.json"

    with pytest.raises(documents.WorkflowDefinitionDocumentError):
        store.delete("target")
    assert target.exists()


def test_delete_preserves_target_when_unrelated_referrer_becomes_unavailable(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    store.save("target", definition("target"))
    store.save("unrelated", definition("unrelated"))
    target = store.definitions / "target.json"
    original = store._read_definition

    def unavailable(name, **kwargs):
        if name == "unrelated":
            raise documents.WorkflowDefinitionDocumentError(name, "source disappeared", code="workflow_definition_unavailable")
        return original(name, **kwargs)

    monkeypatch.setattr(store, "_read_definition", unavailable)
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.delete("target")
    assert caught.value.code == "workflow_definition_unavailable"
    assert target.exists()


def test_qualified_old_definition_without_workflow_id_or_execution_mode_keeps_identity_and_revision(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    old = definition("fixture")
    old["revision"] = 4
    write_json(store.definitions / "fixture.json", documents.encode(old))

    loaded = store.get("fixture")
    assert loaded["workflow_id"].startswith("legacy-")
    assert loaded["nodes"][1]["execution_mode"] == "headless"
    saved = store.save("fixture", definition("fixture"), expected_revision=4)
    assert saved["revision"] == 5
    assert saved["workflow_id"] == loaded["workflow_id"]
