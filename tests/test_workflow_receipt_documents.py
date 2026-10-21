"""Qualified ownership-receipt envelope and bounded-source regressions."""

from __future__ import annotations

import copy
import json
import os
from pathlib import Path

import pytest

from harflow_mcp import bounded_io, workflow_receipt_documents, workflow_receipt_io

from workflow_receipt_fixtures import qualified_receipt


def test_encode_decode_preserves_inner_receipt_and_outer_identity() -> None:
    inner = {
        "workflow_run_id": "run-1",
        "workflow_node_id": "review",
        "workflow_execution_id": "execution",
        "workflow_role": "native_control",
        "workflow_name": None,
        "workflow_status": "running",
        "execution_contract": None,
        "interaction_owner": None,
        "root_workflow_run_id": "run-1",
        "workflow_session_owner_run_id": "run-1",
        "future_receipt_field": {"kept": [True, "as-authored"]},
    }
    encoded = workflow_receipt_documents.encode(inner, "task.with.dots")

    assert encoded["product"] == "harflow"
    assert encoded["format"] == workflow_receipt_documents.RECEIPT_FORMAT
    assert encoded["v"] == 1
    assert encoded["task_id"] == "task.with.dots"
    assert encoded["receipt"] == inner
    decoded = workflow_receipt_documents.decode(encoded, "task.with.dots")
    assert decoded == inner
    assert decoded is not inner


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value.pop("product"),
        lambda value: value.update(product="foreign"),
        lambda value: value.update(format="harflow.workflow.run"),
        lambda value: value.update(v=True),
        lambda value: value.update(v=2),
        lambda value: value.update(product_version="2.0.0"),
        lambda value: value.update(task_id="other"),
        lambda value: value.update(receipt=[]),
    ],
)
def test_decode_rejects_unqualified_envelopes_without_normalizing(mutator) -> None:
    document = qualified_receipt("task", "run")
    mutator(document)
    before = copy.deepcopy(document)

    with pytest.raises(workflow_receipt_documents.WorkflowReceiptDocumentError) as caught:
        workflow_receipt_documents.decode(document, "task")

    assert caught.value.code == "workflow_receipt_incompatible"
    assert caught.value.task_id == "task"
    assert document == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("workflow_run_id", None),
        ("workflow_node_id", 1),
        ("workflow_execution_id", False),
        ("workflow_role", []),
        ("root_workflow_run_id", "not valid id"),
        ("workflow_session_owner_run_id", 2),
    ],
)
def test_decode_rejects_invalid_known_receipt_fields(field, value) -> None:
    inner = {"workflow_run_id": "run", field: value}
    document = workflow_receipt_documents.encode({"workflow_run_id": "run"}, "task")
    document["receipt"] = inner

    with pytest.raises(workflow_receipt_documents.WorkflowReceiptDocumentError):
        workflow_receipt_documents.decode(document, "task")


def test_read_receipt_returns_inner_payload_and_keeps_public_budget(tmp_path: Path) -> None:
    path = tmp_path / "task.json"
    path.write_text(json.dumps(qualified_receipt("task", "run")), encoding="utf-8")

    assert bounded_io.read_receipt(path) == {"workflow_run_id": "run"}
    assert bounded_io.RECEIPT_BYTES == 16 * 1024


def test_receipt_read_distinguishes_verified_absence_from_dangling_source(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    dangling = tmp_path / "dangling.json"
    dangling.symlink_to(tmp_path / "not-there.json")

    assert workflow_receipt_io.observe(missing) is None
    with pytest.raises(workflow_receipt_documents.WorkflowReceiptDocumentError) as caught:
        workflow_receipt_io.observe(dangling)
    assert caught.value.code == "workflow_receipt_unavailable"


def test_receipt_source_replacement_is_unavailable_and_original_identity_is_not_reused(tmp_path: Path) -> None:
    path = tmp_path / "task.json"
    path.write_text(json.dumps(qualified_receipt("task", "run")), encoding="utf-8")
    observed = workflow_receipt_io.observe(path)
    replacement = tmp_path / "replacement.json"
    replacement.write_text(json.dumps(qualified_receipt("task", "other-run")), encoding="utf-8")
    os.replace(replacement, path)

    with pytest.raises(workflow_receipt_documents.WorkflowReceiptDocumentError) as caught:
        workflow_receipt_io.check_source(path, observed)
    assert caught.value.code == "workflow_receipt_unavailable"


def test_malformed_and_oversized_receipts_remain_distinct(tmp_path: Path) -> None:
    malformed = tmp_path / "malformed.json"
    malformed.write_text("{", encoding="utf-8")
    with pytest.raises(workflow_receipt_documents.WorkflowReceiptDocumentError) as malformed_error:
        bounded_io.read_receipt(malformed)
    assert malformed_error.value.code == "workflow_receipt_incompatible"

    oversized = tmp_path / "oversized.json"
    oversized.write_bytes(b"x" * (bounded_io.RECEIPT_BYTES + 1))
    with pytest.raises(bounded_io.ReadLimit):
        bounded_io.read_receipt(oversized)
