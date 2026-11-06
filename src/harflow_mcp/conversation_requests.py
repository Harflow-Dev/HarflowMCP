"""Strict caller commands for the negotiated conversation delivery contract.

These documents express caller intent only. They contain no launch authority,
native acceptance, consumption, settlement or workflow-result evidence.
"""
from __future__ import annotations

import json

from .conversation_documents import (
    CONTRACT, ConversationError, MAX_MESSAGE_BYTES, identifier, request_uuid,
    utf8_size,
)


def validate_send(value: object) -> dict:
    """Validate without normalizing caller prose or resolving another target."""
    if type(value) is not dict or set(value) != {
        "contract", "request_id", "delivery", "text", "target", "expected",
    }:
        raise ConversationError("invalid_request")
    if value["contract"] != CONTRACT or value["delivery"] not in ("queue", "now"):
        raise ConversationError("invalid_request")
    request_uuid(value["request_id"])
    text = value["text"]
    if type(text) is not str or not text.strip() or utf8_size(text) > MAX_MESSAGE_BYTES:
        raise ConversationError("invalid_request")
    _validate_target_and_witness(value)
    return value


def _validate_target_and_witness(value):
    if value["contract"] != CONTRACT:
        raise ConversationError("invalid_request")
    request_uuid(value["request_id"])
    target = value["target"]
    if type(target) is not dict:
        raise ConversationError("invalid_request")
    if target.get("kind") == "task" and set(target) == {"kind", "task_id"}:
        identifier(target["task_id"])
    elif target.get("kind") == "workflow_activation" and set(target) == {
        "kind", "workflow_run_id", "activation_id", "task_id",
    }:
        for key in ("workflow_run_id", "activation_id", "task_id"):
            identifier(target[key])
    else:
        raise ConversationError("invalid_request")
    expected = value["expected"]
    if type(expected) is not dict or set(expected) != {
        "conversation_id", "head_task_id", "generation", "turn_id", "ledger_epoch",
    }:
        raise ConversationError("invalid_request")
    request_uuid(expected["conversation_id"])
    identifier(expected["head_task_id"])
    identifier(expected["turn_id"])
    for key in ("generation", "ledger_epoch"):
        if type(expected[key]) is not int or expected[key] < 0:
            raise ConversationError("invalid_request")


def _load_document(path):
    """Read a bounded explicit caller file; duplicate JSON fields are ambiguous."""
    def invalid_constant(_):
        raise ConversationError("invalid_request")

    def pairs(entries):
        value = {}
        for key, item in entries:
            if key in value:
                raise ConversationError("invalid_request")
            value[key] = item
        return value
    with path.open("rb") as stream:
        raw = stream.read(64 * 1024 + 1)
    if len(raw) > 64 * 1024:
        raise ConversationError("request_too_large")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs,
                           parse_constant=invalid_constant)
    except (UnicodeError, ValueError, RecursionError) as exc:
        raise ConversationError("invalid_request") from exc
    return value


def validate_control(value: object) -> dict:
    """Controls carry new operation UUIDs and an exact existing message target."""
    if type(value) is not dict or set(value) != {
        'contract', 'request_id', 'operation', 'target', 'expected', 'message_request_id',
    } or value['operation'] not in ('cancel', 'promote_now'):
        raise ConversationError('invalid_request')
    request_uuid(value['message_request_id'])
    _validate_target_and_witness(value)
    return value


def validate_commit(value: object) -> dict:
    """An explicit, finite workflow commit is distinct from conversational prose."""
    if type(value) is not dict or set(value) != {
        'contract', 'request_id', 'operation', 'target', 'expected', 'steering_request',
    } or value['operation'] != 'commit':
        raise ConversationError('invalid_request')
    _validate_target_and_witness(value)
    if value['target']['kind'] != 'workflow_activation':
        raise ConversationError('workflow_activation_required')
    request = value['steering_request']
    if type(request) is not dict or request.get('request_id') != value['request_id']:
        raise ConversationError('request_conflict')
    # Existing steering owns the complete inner shape, finite cap and no-Fresh decision.
    from .workflow_steering import _request
    _request(request)
    return value


def load_send(path):
    return validate_send(_load_document(path))


def load_control(path):
    return validate_control(_load_document(path))


def load_commit(path):
    return validate_commit(_load_document(path))
