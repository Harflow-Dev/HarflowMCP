"""Caller intent cannot smuggle transport evidence or broaden a target."""
from copy import deepcopy
import uuid

import pytest

from harflow_mcp.conversation_documents import CONTRACT, ConversationError
from harflow_mcp.conversation_requests import load_send, validate_send


def command():
    return {
        "contract": CONTRACT, "request_id": str(uuid.uuid4()),
        "delivery": "queue", "text": "  Keep these spaces.\nNext line.  ",
        "target": {"kind": "task", "task_id": "source"},
        "expected": {"conversation_id": str(uuid.uuid4()), "head_task_id": "head",
                     "generation": 2, "turn_id": "turn", "ledger_epoch": 0},
    }


def test_prose_and_identities_are_preserved():
    value = command()
    original = deepcopy(value)
    assert validate_send(value) is value
    assert value == original


@pytest.mark.parametrize("change", [
    lambda c: c.update(consumed=True),
    lambda c: c.update(qualified=True),
    lambda c: c["target"].update(node_id="a-different-node"),
    lambda c: c["expected"].update(generation=True),
    lambda c: c["expected"].pop("ledger_epoch"),
    lambda c: c.update(delivery="fresh"),
    lambda c: c.update(text=" \n "),
    lambda c: c.update(text="x" * (32 * 1024 + 1)),
    lambda c: c.update(request_id="not-a-uuid"),
])
def test_invalid_or_authority_bearing_requests_refuse(change):
    value = command()
    change(value)
    with pytest.raises(ConversationError):
        validate_send(value)


@pytest.mark.parametrize("raw", [
    b'{"contract":"first","contract":"second"}',
    b'{"number":NaN}', b'\xff', b' ' * (64 * 1024 + 1),
])
def test_request_files_are_bounded_and_unambiguous(tmp_path, raw):
    path = tmp_path / "request.json"
    path.write_bytes(raw)
    with pytest.raises(ConversationError):
        load_send(path)
