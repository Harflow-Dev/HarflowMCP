"""Qualification and round-trip tests for persisted follow-up ledgers."""

from __future__ import annotations

import copy

import pytest

from harflow_mcp import followup_documents


def ledger(**overrides: object) -> dict:
    value = {
        "v": 1,
        "next_seq": 1,
        "sealed_by": None,
        "items": [],
        "omitted_final": 0,
    }
    value.update(overrides)
    return value


def qualified(task_id: str = "task") -> dict:
    return followup_documents.encode(
        ledger(
            items=[
                {
                    "id": "message",
                    "seq": 1,
                    "status": "queued",
                    "text": "  preserve\nword boundaries  \r\n",
                    "queued_at": "2026-01-01T00:00:00+00:00",
                    "by": {"actor": "caller", "future": [True, None]},
                    "future_item_field": {"nested": [1, "two"]},
                }
            ],
            future_ledger_field={"keep": ["as", "authored"]},
        ),
        task_id,
    )


def test_encode_decode_preserves_inner_ledger_and_identity() -> None:
    document = qualified("task-with-dash")

    assert document["product"] == "harflow"
    assert document["format"] == followup_documents.FOLLOWUP_FORMAT
    assert document["v"] == 1
    assert document["task_id"] == "task-with-dash"
    decoded = followup_documents.decode(document, "task-with-dash")

    assert decoded["items"][0]["text"] == "  preserve\nword boundaries  \r\n"
    assert decoded["future_ledger_field"] == {"keep": ["as", "authored"]}
    assert decoded["items"][0]["future_item_field"] == {"nested": [1, "two"]}
    assert decoded is not document


def test_encode_preserves_additive_outer_fields_without_reusing_identity() -> None:
    envelope = {"future_outer_field": {"kept": True}, "task_id": "wrong"}
    encoded = followup_documents.encode(ledger(), "task", envelope=envelope)

    assert encoded["future_outer_field"] == {"kept": True}
    assert encoded["task_id"] == "task"


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: value.pop("product"),
        lambda value: value.update(product="foreign"),
        lambda value: value.update(format="harflow.workflow.receipt"),
        lambda value: value.update(v=True),
        lambda value: value.update(v=2),
        lambda value: value.update(product_version="2.0.0"),
        lambda value: value.update(task_id="other"),
        lambda value: value.update(ledger=[]),
    ],
)
def test_decode_rejects_unqualified_envelopes_without_normalizing(mutator) -> None:
    document = qualified()
    mutator(document)
    before = copy.deepcopy(document)

    with pytest.raises(followup_documents.FollowupDocumentError):
        followup_documents.decode(document, "task")

    assert document == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("v", True),
        ("next_seq", True),
        ("next_seq", 0),
        ("omitted_final", True),
        ("sealed_by", 1),
        ("items", {}),
    ],
)
def test_decode_rejects_invalid_known_ledger_fields(field: str, value: object) -> None:
    document = qualified()
    document["ledger"][field] = value

    with pytest.raises(followup_documents.FollowupDocumentError):
        followup_documents.decode(document, "task")


@pytest.mark.parametrize(
    "item",
    [
        {"id": [], "seq": 1, "status": "queued", "text": "x"},
        {"id": "m", "seq": True, "status": "queued", "text": "x"},
        {"id": "m", "seq": 1, "status": "unknown", "text": "x"},
        {"id": "m", "seq": 1, "status": "queued", "text": 1},
        {"id": "m", "seq": 1, "status": "consumed", "text": 1},
    ],
)
def test_decode_rejects_invalid_known_item_fields(item: dict) -> None:
    document = qualified()
    document["ledger"]["items"] = [item]

    with pytest.raises(followup_documents.FollowupDocumentError):
        followup_documents.decode(document, "task")


def test_decode_rejects_invalid_known_optional_types() -> None:
    for field, value in (
        ("queued_at", 1),
        ("at", []),
        ("reason", {}),
        ("successor_task_id", 1),
        ("by", []),
        ("owner", "owner"),
    ):
        document = qualified()
        document["ledger"]["items"][0][field] = value
        with pytest.raises(followup_documents.FollowupDocumentError):
            followup_documents.decode(document, "task")


def test_decode_preserves_owner_and_by_objects_without_authority_validation() -> None:
    document = qualified()
    owner = {"pid": "not-runtime-authority", "start_time": 1, "markers": [1, {"nested": True}]}
    by = {"arbitrary": [None, {"future": "shape"}]}
    document["ledger"]["items"][0]["owner"] = owner
    document["ledger"]["items"][0]["by"] = by

    decoded = followup_documents.decode(document, "task")
    assert decoded["items"][0]["owner"] == owner
    assert decoded["items"][0]["by"] == by

