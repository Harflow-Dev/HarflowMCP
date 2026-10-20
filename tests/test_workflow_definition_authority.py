"""Authority races and adapter error contracts for saved definitions."""

from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp import MCPError

from harflow_mcp import ctl, server, workflow_definition_documents as documents, workflow_delegation, workflows


def definition(name: str = "fixture") -> dict:
    return {
        "name": name,
        "routing_mode": "explicit",
        "orchestrator": {"backend": "codex"},
        "nodes": [
            {"id": "start", "type": "start"},
            {"id": "work", "type": "agent", "role": "task", "instructions": "Do the work", "agent": {"backend": "codex"}},
            {"id": "end", "type": "end"},
        ],
        "connections": [
            {"id": "begin", "source": "start", "target": "work"},
            {"id": "finish", "source": "work", "target": "end"},
        ],
    }


async def test_mcp_definition_refusal_preserves_structured_name_code_reason(monkeypatch):
    async def refuse(*args, **kwargs):
        raise documents.WorkflowDefinitionDocumentError("fixture", "raw source", code="workflow_definition_incompatible")

    monkeypatch.setattr(server, "_workflow_call", refuse)
    with pytest.raises(MCPError) as caught:
        await server.list_workflows()
    assert caught.value.data == {
        "name": "fixture",
        "code": "workflow_definition_incompatible",
        "reason": "raw source",
    }


def test_ctl_definition_refusal_keeps_stable_code_and_does_not_reclassify_as_missing(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    original = workflows.WorkflowStore._read_definition

    def refuse(self, name, **kwargs):
        if name == "fixture":
            raise documents.WorkflowDefinitionDocumentError(name, "raw source", code="workflow_definition_incompatible")
        return original(self, name, **kwargs)

    monkeypatch.setattr(workflows.WorkflowStore, "_read_definition", refuse)
    assert ctl.main(["workflow-get", "fixture", "--json"]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["error"]["code"] == "workflow_definition_incompatible"
    assert "missing" not in output["error"]["code"]


def test_ctl_definition_unavailable_remains_distinct_from_incompatible(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))

    def refuse(self, name, **kwargs):
        raise documents.WorkflowDefinitionDocumentError(name, "source changed", code="workflow_definition_unavailable")

    monkeypatch.setattr(workflows.WorkflowStore, "_read_definition", refuse)
    assert ctl.main(["workflow-get", "fixture", "--json"]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["error"]["code"] == "workflow_definition_unavailable"


def test_raw_definition_file_is_preserved_when_cli_refuses_it(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HOME", str(tmp_path))
    store = workflows.WorkflowStore(tmp_path / ".harflow")
    path = store.definitions / "fixture.json"
    raw = definition()
    path.write_text(json.dumps(raw), encoding="utf-8")
    before = path.read_bytes()
    assert ctl.main(["workflow-get", "fixture", "--json"]) == 1
    capsys.readouterr()
    assert path.read_bytes() == before


def test_get_refuses_symlinked_definition_source(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(documents.encode(definition())), encoding="utf-8")
    (store.definitions / "fixture.json").symlink_to(outside)
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.get("fixture")
    assert caught.value.code == "workflow_definition_unavailable"


def test_get_refuses_deep_json_source_as_typed_incompatible_and_preserves_bytes(tmp_path):
    store = workflows.WorkflowStore(tmp_path / "state")
    path = store.definitions / "fixture.json"
    deep = "{" + '"next":{' * 2500 + "}" * 2500 + "}"
    path.write_text(deep, encoding="utf-8")
    before = path.read_bytes()
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.get("fixture")
    assert caught.value.code == "workflow_definition_incompatible"
    assert path.read_bytes() == before


def test_verified_missing_definition_keeps_file_not_found_semantics(tmp_path):
    store = workflows.WorkflowStore(tmp_path / "state")
    with pytest.raises(FileNotFoundError):
        store.get("missing")


def test_list_refuses_definition_that_disappears_after_discovery(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    store.save("fixture", definition())
    original = store._read_definition
    path = store.definitions / "fixture.json"

    def disappears(name, **kwargs):
        if name == "fixture":
            path.unlink()
            return original(name, **kwargs)
        return original(name, **kwargs)

    monkeypatch.setattr(store, "_read_definition", disappears)
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.list()
    assert caught.value.code == "workflow_definition_unavailable"


async def test_mcp_save_refuses_raw_neighbor_with_structured_error_and_preserves_bytes(tmp_path, monkeypatch):
    root = tmp_path / ".harflow"
    store = workflows.WorkflowStore(root)
    store.save("target", definition("target"))
    neighbor = store.definitions / "raw-neighbor.json"
    neighbor.write_text(json.dumps(definition("raw-neighbor")), encoding="utf-8")
    before = neighbor.read_bytes()
    monkeypatch.setattr(server, "_reg", lambda: SimpleNamespace(log_dir=root / "tasks"))

    async def no_mutation_guard():
        return None

    monkeypatch.setattr(server, "_guard_mutation", no_mutation_guard)
    monkeypatch.setattr(server, "_verified_workflow_caller", no_mutation_guard)
    monkeypatch.setattr(server, "_managed_workflow_reader", no_mutation_guard)
    with pytest.raises(MCPError) as caught:
        await server.save_workflow("new", definition("new"))
    assert caught.value.data["name"] == "raw-neighbor"
    assert caught.value.data["code"] == "workflow_definition_incompatible"
    assert caught.value.data["reason"]
    assert neighbor.read_bytes() == before


def test_ctl_delete_refuses_raw_neighbor_with_stable_code_and_preserves_bytes(tmp_path, monkeypatch, capsys):
    root = server._reg().log_dir.parent
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("HARFLOW_STATE_ROOT", str(root))
    store = workflows.WorkflowStore(root)
    store.save("target", definition("target"))
    neighbor = store.definitions / "raw-neighbor.json"
    neighbor.write_text(json.dumps(definition("raw-neighbor")), encoding="utf-8")
    before = neighbor.read_bytes()
    assert ctl.main(["workflow-delete", "target", "--json"]) == 1
    output = json.loads(capsys.readouterr().out)
    assert output["error"]["code"] == "workflow_definition_incompatible"
    assert "missing" not in output["error"]["code"]
    assert (store.definitions / "target.json").exists()
    assert neighbor.read_bytes() == before


def test_save_refuses_absent_destination_that_appears_as_same_name_with_different_identity(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    target = store.definitions / "fixture.json"
    different = definition("fixture")
    different["workflow_id"] = "different-id"

    def guard(_tree):
        target.write_text(json.dumps(documents.encode(different)), encoding="utf-8")

    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        store.save("fixture", definition(), authority_guard=guard)
    assert caught.value.code == "workflow_definition_unavailable"
    assert json.loads(target.read_text())["definition"]["workflow_id"] == "different-id"


def test_migration_refuses_incompatible_source_without_overwriting_it(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    payload = definition()
    payload["revision"] = 1
    path = store.definitions / "fixture.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    before = path.read_bytes()

    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        workflow_delegation.migrate(store)
    assert caught.value.code == "workflow_definition_incompatible"
    assert path.read_bytes() == before


def test_migration_rechecks_replaced_source_before_backup_write(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    payload = definition()
    payload["revision"] = 1
    path = store.definitions / "fixture.json"
    path.write_text(json.dumps(documents.encode(payload)), encoding="utf-8")
    replacement = definition("fixture")
    replacement["workflow_id"] = "replacement-id"
    replacement_path = tmp_path / "replacement.json"
    replacement_path.write_text(json.dumps(documents.encode(replacement)), encoding="utf-8")
    original_project = store._project_definition
    projections = 0

    def replace_after_migration_qualification(value):
        nonlocal projections
        projections += 1
        result = original_project(value)
        if projections == 2:
            os.replace(replacement_path, path)
        return result

    monkeypatch.setattr(store, "_project_definition", replace_after_migration_qualification)
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        workflow_delegation.migrate(store)
    assert caught.value.code == "workflow_definition_unavailable"
    assert json.loads(path.read_text())["definition"]["workflow_id"] == "replacement-id"
    backup_dir = store.root / "workflow-backups"
    assert not list(backup_dir.glob("*.json"))


def test_migration_final_replace_guard_preserves_replacement_after_backup(tmp_path, monkeypatch):
    monkeypatch.setattr(workflows.backends, "is_installed", lambda backend: True)
    store = workflows.WorkflowStore(tmp_path / "state")
    payload = definition()
    payload["revision"] = 1
    path = store.definitions / "fixture.json"
    path.write_text(json.dumps(documents.encode(payload)), encoding="utf-8")
    expected_backup = store.get("fixture")
    replacement = definition("fixture")
    replacement["workflow_id"] = "replacement-id"
    replacement_path = tmp_path / "replacement.json"
    replacement_path.write_text(json.dumps(documents.encode(replacement)), encoding="utf-8")
    original_write = workflows._write

    def replace_during_final_write(path_arg, value, *, layout, before_replace=None):
        if path_arg == path:
            original_callback = before_replace

            def replace_then_check():
                os.replace(replacement_path, path)
                original_callback()

            before_replace = replace_then_check
        return original_write(path_arg, value, layout=layout, before_replace=before_replace)

    monkeypatch.setattr(workflows, "_write", replace_during_final_write)
    with pytest.raises(documents.WorkflowDefinitionDocumentError) as caught:
        workflow_delegation.migrate(store)
    assert caught.value.code == "workflow_definition_unavailable"
    assert json.loads(path.read_text())["definition"]["workflow_id"] == "replacement-id"
    backups = list((store.root / "workflow-backups").glob("*.json"))
    assert len(backups) == 1
    assert json.loads(backups[0].read_text()) == documents.encode(expected_backup)
    assert not list(store.definitions.glob(".*.tmp"))

