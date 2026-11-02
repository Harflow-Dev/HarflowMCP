"""Measured Vibe denial schema and conservative live-turn handling."""
import json
from pathlib import Path

import pytest

from harflow_mcp.backends import Accumulator
from harflow_mcp.backends.vibe import VibeBackend
from harflow_mcp.backends.vibe_denials import classify_one

BACKEND = VibeBackend()


def capture(name):
    return json.loads((Path(__file__).parent / "fixtures" / "vibe-denials" / name).read_text())


def ingest(events):
    acc = Accumulator()
    for event in events:
        BACKEND.ingest(event, acc)
    return acc


@pytest.mark.parametrize("text,category", [
    ("Tool execution denied by approval policy", "policy_denied"),
    ("Tool execution denied by approval callback", "approval_required"),
    ("Command denied: 'touch x' matches denylist pattern 'touch'. Do not attempt to run this command.", "deny_rule"),
    ("Command denied: 'touch' is not allowed as a standalone command. Do not attempt to run this command.", "deny_rule"),
    ("future reason", "unclassified"),
])
def test_exact_structured_text(text, category):
    assert classify_one({"error_code": "tool_denied", "refusal_text": text})["denial_category"] == category
    assert classify_one({"refusal_text": text})["denial_category"] == "unclassified"


def stream():
    return [
        {"type": "message", "role": "user", "source": "turn_start", "turnId": "live", "sessionId": "s"},
        {"type": "callback", "turnId": "live", "sessionId": "s", "relatedEntryId": "e", "detail": {"kind": "approval", "requiredPermissions": [{"scope": "command_pattern", "invocationPattern": "touch x", "sessionPattern": "touch *"}]}},
        {"type": "effect", "id": "e", "turnId": "live", "sessionId": "s", "detail": {"toolName": "file_system.bash", "input": {"command": "touch x"}}, "state": {"status": "failed", "error": {"code": "tool_denied", "message": "Tool execution denied by approval callback"}}},
    ]


def answer():
    return {"type": "message", "role": "assistant", "turnId": "live", "sessionId": "s", "content": [{"type": "text", "text": "recovered"}]}


@pytest.mark.parametrize("reverse", [False, True])
def test_identity_dedup_does_not_withdraw_recovery(reverse):
    marker, callback, effect = stream()
    first, second = (effect, callback) if reverse else (callback, effect)
    acc = ingest([marker, first, answer(), second, second])
    assert len(acc.denials) == 1
    assert acc.summary == "recovered" and not acc.notices
    assert BACKEND.classify_denials(acc.denials, acc)[0]["denial_category"] == "approval_required"


@pytest.mark.parametrize("field,value", [("turnId", None), ("turnId", "stale"), ("sessionId", "foreign")])
def test_effect_replay_and_foreign_session(field, value):
    marker, _, effect = stream()
    effect[field] = value
    assert not ingest([marker, effect]).denials
    assert not ingest([effect]).denials


def test_distinct_effect_refuses_and_failure_precedence():
    marker, _, effect = stream()
    acc = ingest([marker, answer(), effect])
    assert acc.summary is None and acc.notices
    assert BACKEND.classify(acc, 0) == "completed"
    assert BACKEND.classify(acc, 1) == "failed"
    assert BACKEND.classify(acc, None) == "failed"
    BACKEND.ingest(answer(), acc)
    assert acc.summary == "recovered" and not acc.notices
    marker = {**marker, "turnId": "next"}
    BACKEND.ingest(marker, acc)
    assert not acc.denials


@pytest.mark.parametrize("error", [None, {}, {"code": "other"}, {"message": "Tool execution denied by approval policy"}])
def test_malformed_effect_is_not_a_denial(error):
    marker, _, effect = stream()
    effect["state"]["error"] = error
    assert not ingest([marker, effect]).denials


def test_legacy_callback_raw_payload_stays_equal_and_unclassified():
    marker, callback, _ = stream()
    callback["detail"].pop("requiredPermissions")
    acc = ingest([marker, callback])
    assert acc.denials == [{"kind": "approval"}]
    assert BACKEND.classify_denials(acc.denials, acc)[0]["denial_category"] == "unclassified"


@pytest.mark.parametrize("name,category", [("policy-never", "policy_denied"), ("bash-denylist", "deny_rule"), ("standalone-denylist", "deny_rule"), ("command-pattern", "approval_required"), ("file-pattern", "approval_required"), ("outside-directory", "approval_required")])
def test_actual_captured_fixture(name, category):
    fixture = capture(name + ".json")
    marker = {"type": "message", "role": "user", "source": "turn_start", "turnId": "fixture-turn", "sessionId": "fixture-session"}
    acc = ingest([marker, *fixture["entries"]])
    assert len(acc.denials) == 1
    denial = BACKEND.classify_denials(acc.denials, acc)[0]
    assert denial["denial_category"] == category
    assert denial["classification_basis"]["measured_versions"] == ["3.0.3"]
    assert "version" not in denial["classification_basis"]


def test_command_equality_never_deduplicates_distinct_ids():
    marker, callback, effect = stream()
    effect['id'] = 'other'
    assert len(ingest([marker, callback, effect]).denials) == 2


def test_conflicting_identity_does_not_mix_actions():
    marker, callback, effect = stream()
    callback['detail']['effect'] = {'toolName': 'file_system.bash', 'input': {'command': 'different'}}
    acc = ingest([marker, callback, effect])
    assert len(acc.denials) == 2
    classified = BACKEND.classify_denials(acc.denials, acc)
    assert classified[1]['denial_category'] == 'unclassified'
    assert classified[0]['command'] == 'different'


def test_cap_still_wins_after_refusal_and_recovery():
    marker, _, effect = stream()
    cap = answer()
    cap['content'][0]['text'] = '<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>'
    acc = ingest([marker, effect, cap, answer()])
    assert BACKEND.classify(acc, 0) == 'failed'


@pytest.mark.parametrize('permissions', [[], [None], [{'scope': 'future', 'invocationPattern': 'x', 'sessionPattern': '*'}], [{'scope': 'command_pattern'}], [{'scope': [], 'invocationPattern': 'x', 'sessionPattern': '*'}], [{'scope': {}, 'invocationPattern': 'x', 'sessionPattern': '*'}]])
def test_unknown_or_malformed_callback_scope_stays_unclassified(permissions):
    marker, callback, _ = stream()
    callback['detail']['requiredPermissions'] = permissions
    acc = ingest([marker, callback])
    assert BACKEND.classify_denials(acc.denials, acc)[0]['denial_category'] == 'unclassified'
