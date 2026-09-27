"""The normalizer seam: `backends.normalize`'s shared builders, and each backend's own `normalize`.

Fixtures below use the real shapes documented in each backend's own module (claude stream-json,
codex `exec --json`, opencode `run --format json`, vibe `--output streaming`), trimmed to the
fields `normalize` actually reads — the same convention `tests/test_backends.py` uses for its own
stream fixtures.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone

import pytest

from polybridge import backends
from polybridge.backends import Accumulator
from polybridge.backends.claude import ClaudeBackend
from polybridge.backends.codex import CodexBackend
from polybridge.backends.normalize import (
    CATEGORIES,
    EDIT_TEXT_BYTES,
    INPUT_PREVIEW_BYTES,
    OUTPUT_TAIL_BYTES,
    iso_from_epoch_ms,
    iso_string,
    preview,
    tool_call,
    tool_result,
    truncate_head,
    truncate_tail,
)
from polybridge.backends.opencode import OpencodeBackend
from polybridge.backends.vibe import VibeBackend

SESSION = "11111111-1111-1111-1111-111111111111"


def run(backend, events: list[dict]) -> tuple[Accumulator, list[dict]]:
    """Feed events through `ingest` then `normalize`, in stream order — the order the drain path
    itself uses, since `normalize` is called after `ingest` has folded the same event into `acc`."""
    acc = Accumulator()
    produced: list[dict] = []
    for event in events:
        # Round-trip through JSON, same as test_backends.ingest, so a fixture accidentally sharing
        # a mutable object across events cannot leak state between them.
        event = json.loads(json.dumps(event))
        backend.ingest(event, acc)
        produced.extend(backend.normalize(event, acc))
    return acc, produced


def _acc_snapshot_excluding_normalize_state(acc: Accumulator) -> dict:
    data = dataclasses.asdict(acc)
    data["stream_state"] = {
        key: value for key, value in data["stream_state"].items() if not key.startswith("normalize_")
    }
    return data


# ---------------------------------------------------------------------------------------------
# claude
# ---------------------------------------------------------------------------------------------

CLAUDE_ASSISTANT_EVENT = {
    "type": "assistant",
    "session_id": SESSION,
    "timestamp": "2026-09-24T10:00:00.000Z",
    "message": {
        "content": [
            {"type": "text", "text": "Let me edit the file."},
            {"type": "thinking", "thinking": "internal reasoning, never surfaced"},
            {"type": "tool_use", "id": "toolu_1", "name": "Edit",
             "input": {"file_path": "/tmp/a.py", "old_string": "foo", "new_string": "bar"}},
            {"type": "tool_use", "id": "toolu_2", "name": "Bash", "input": {"command": "echo hi"}},
            {"type": "tool_use", "id": "toolu_3", "name": "mcp__owlex__search",
             "input": {"query": "x"}},
        ]
    },
}

CLAUDE_USER_TOOL_RESULT_STR = {
    "type": "user",
    "session_id": SESSION,
    "timestamp": "2026-09-24T10:00:01.000Z",
    "message": {"content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "edited ok"}]},
}

CLAUDE_USER_TOOL_RESULT_LIST_ERROR = {
    "type": "user",
    "session_id": SESSION,
    "message": {
        "content": [
            {"type": "tool_result", "tool_use_id": "toolu_2", "is_error": True,
             "content": [{"type": "text", "text": "line1"}, {"type": "text", "text": "line2"}]},
        ]
    },
}

CLAUDE_USER_SYNTHETIC = {
    "type": "user",
    "isSynthetic": True,
    "message": {"content": [{"type": "text", "text": "a skill body injected by the harness"}]},
}

CLAUDE_USER_TEXT = {
    "type": "user",
    "session_id": SESSION,
    "message": {"content": "Please continue"},
}

CLAUDE_SUBAGENT_EVENT = {
    "type": "assistant",
    "parent_tool_use_id": "toolu_sub",
    "message": {
        "content": [
            {"type": "text", "text": "subagent's own prose, never the main thread speaking"},
            {"type": "tool_use", "id": "toolu_sub_1", "name": "Read", "input": {"file_path": "/tmp/b.py"}},
        ]
    },
}

CLAUDE_RESULT_EVENT = {
    "type": "result",
    "subtype": "success",
    "session_id": SESSION,
    "result": "done",
    "is_error": False,
    "num_turns": 2,
    "total_cost_usd": 0.1,
    "usage": {"input_tokens": 100, "output_tokens": 20},
}


def test_claude_text_and_tool_use_produce_assistant_text_and_tool_calls() -> None:
    _acc, produced = run(ClaudeBackend(), [CLAUDE_ASSISTANT_EVENT])

    # The thinking block is never surfaced, so exactly one text and three tool_use blocks land.
    assert [event["kind"] for event in produced] == [
        "assistant_text", "tool_call", "tool_call", "tool_call",
    ]
    assert produced[0]["text"] == "Let me edit the file."
    assert produced[0]["source_ts"] == "2026-09-24T10:00:00.000Z"

    edit_call = produced[1]
    assert edit_call["tool"] == "Edit"
    assert edit_call["category"] == "edit"
    assert edit_call["path"] == "/tmp/a.py"
    assert edit_call["edit"] == {"old": "foo", "new": "bar"}

    bash_call = produced[2]
    assert bash_call["tool"] == "Bash"
    assert bash_call["category"] == "shell"
    assert bash_call["command"] == "echo hi"

    mcp_call = produced[3]
    assert mcp_call["tool"] == "mcp__owlex__search"
    assert mcp_call["category"] == "mcp"


def test_claude_tool_result_str_content() -> None:
    _acc, produced = run(ClaudeBackend(), [CLAUDE_USER_TOOL_RESULT_STR])

    assert produced == [
        {"kind": "tool_result", "call_id": "toolu_1", "ok": True, "output_tail": "edited ok",
         "source_ts": "2026-09-24T10:00:01.000Z"}
    ]


def test_claude_tool_result_list_content_is_joined_and_is_error_flips_ok() -> None:
    _acc, produced = run(ClaudeBackend(), [CLAUDE_USER_TOOL_RESULT_LIST_ERROR])

    assert len(produced) == 1
    assert produced[0]["ok"] is False
    assert produced[0]["output_tail"] == "line1line2"


def test_claude_synthetic_user_text_is_dropped() -> None:
    """isSynthetic user text is a skill body injected by the harness, not the user speaking."""
    _acc, produced = run(ClaudeBackend(), [CLAUDE_USER_SYNTHETIC])

    assert produced == []


def test_claude_str_message_content_is_a_user_text() -> None:
    _acc, produced = run(ClaudeBackend(), [CLAUDE_USER_TEXT])

    assert produced == [{"kind": "user_message", "text": "Please continue", "source": "initial"}]


def test_claude_subagent_scope_drops_prose_but_keeps_tool_use() -> None:
    _acc, produced = run(ClaudeBackend(), [CLAUDE_SUBAGENT_EVENT])

    assert [event["kind"] for event in produced] == ["tool_call"]
    assert produced[0]["tool"] == "Read"
    assert produced[0]["path"] == "/tmp/b.py"


def test_claude_result_emits_cumulative_usage() -> None:
    acc, produced = run(ClaudeBackend(), [CLAUDE_RESULT_EVENT])

    assert acc.usage == {"input_tokens": 100, "output_tokens": 20}
    assert produced == [
        {"kind": "usage", "usage": {"input_tokens": 100, "output_tokens": 20},
         "total_cost_usd": 0.1, "num_turns": 2}
    ]


# ---------------------------------------------------------------------------------------------
# codex
# ---------------------------------------------------------------------------------------------

CODEX_STARTED_COMMAND = {
    "type": "item.started",
    "item": {"id": "item_a", "type": "command_execution", "command": "ls -la"},
}
CODEX_COMPLETED_COMMAND = {
    "type": "item.completed",
    "item": {"id": "item_a", "type": "command_execution", "command": "ls -la",
              "exit_code": 0, "status": "completed", "aggregated_output": "file1\nfile2\n"},
}
CODEX_COMPLETED_COMMAND_NO_START = {
    "type": "item.completed",
    "item": {"id": "item_b", "type": "command_execution", "command": "pwd",
              "exit_code": 1, "status": "failed", "aggregated_output": "err"},
}

CODEX_MCP_STARTED = {
    "type": "item.started",
    "item": {"id": "item_c", "type": "mcp_tool_call", "server": "owlex", "tool": "search",
              "arguments": {"q": "hi"}},
}
CODEX_MCP_COMPLETED_OK = {
    "type": "item.completed",
    "item": {"id": "item_c", "type": "mcp_tool_call", "server": "owlex", "tool": "search",
              "status": "completed",
              "result": {"content": [{"type": "text", "text": "result1"},
                                      {"type": "text", "text": "result2"}]}},
}
CODEX_MCP_COMPLETED_ERR_NO_START = {
    "type": "item.completed",
    "item": {"id": "item_d", "type": "mcp_tool_call", "server": "owlex", "tool": "search",
              "status": "failed", "error": {"message": "boom"}},
}

CODEX_AGENT_MESSAGE = {
    "type": "item.completed",
    "item": {"id": "item_e", "type": "agent_message", "text": "ok done"},
}
CODEX_ERROR_ITEM = {
    "type": "item.completed",
    "item": {"id": "item_f", "type": "error", "message": "Skill descriptions were shortened"},
}
CODEX_TURN_COMPLETED = {"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 5}}
CODEX_TOP_ERROR_TOP_MESSAGE = {"type": "error", "message": "top level oops"}
CODEX_TOP_ERROR_NESTED_MESSAGE = {"type": "error", "error": {"message": "nested oops"}}
CODEX_TURN_FAILED = {"type": "turn.failed", "message": "turn failed reason"}


def test_codex_command_execution_started_then_completed() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_STARTED_COMMAND, CODEX_COMPLETED_COMMAND])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["command"] == "ls -la"
    assert produced[0]["category"] == "shell"
    assert produced[1]["ok"] is True
    assert produced[1]["exit_code"] == 0
    assert produced[1]["output_tail"] == "file1\nfile2\n"


def test_codex_completed_without_started_still_yields_tool_call_then_result() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_COMPLETED_COMMAND_NO_START])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["command"] == "pwd"
    assert produced[1]["ok"] is False
    assert produced[1]["exit_code"] == 1


def test_codex_mcp_tool_call_success() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_MCP_STARTED, CODEX_MCP_COMPLETED_OK])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["tool"] == "owlex.search"
    assert produced[0]["category"] == "mcp"
    assert produced[1]["ok"] is True
    assert produced[1]["output_tail"] == "result1result2"


def test_codex_mcp_tool_call_error_without_started() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_MCP_COMPLETED_ERR_NO_START])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[1]["ok"] is False
    assert produced[1]["output_tail"] == "boom"


def test_codex_agent_message_is_assistant_text() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_AGENT_MESSAGE])

    assert produced == [{"kind": "assistant_text", "text": "ok done"}]


def test_codex_error_item_is_a_notice() -> None:
    """Observed in a genuinely successful run (a benign skills warning) — never proof of failure."""
    _acc, produced = run(CodexBackend(), [CODEX_ERROR_ITEM])

    assert produced == [{"kind": "notice", "text": "Skill descriptions were shortened"}]


def test_codex_turn_completed_emits_cumulative_usage() -> None:
    acc, produced = run(CodexBackend(), [CODEX_TURN_COMPLETED])

    assert acc.usage == {"input_tokens": 100, "output_tokens": 5}
    assert produced == [
        {"kind": "usage", "usage": {"input_tokens": 100, "output_tokens": 5},
         "total_cost_usd": None, "num_turns": 1}
    ]


@pytest.mark.parametrize(
    ("event", "text"),
    [
        (CODEX_TOP_ERROR_TOP_MESSAGE, "top level oops"),
        (CODEX_TOP_ERROR_NESTED_MESSAGE, "nested oops"),
        (CODEX_TURN_FAILED, "turn failed reason"),
    ],
)
def test_codex_top_level_error_and_failed_events_are_notices(event: dict, text: str) -> None:
    _acc, produced = run(CodexBackend(), [event])

    assert produced == [{"kind": "notice", "text": text}]


def test_codex_normalize_never_mutates_acc_beyond_normalize_prefixed_stream_state() -> None:
    acc = Accumulator()
    codex = CodexBackend()
    event = json.loads(json.dumps(CODEX_STARTED_COMMAND))
    codex.ingest(event, acc)
    before = _acc_snapshot_excluding_normalize_state(acc)

    codex.normalize(event, acc)

    after = _acc_snapshot_excluding_normalize_state(acc)
    assert before == after


# ---------------------------------------------------------------------------------------------
# codex file_change — codex used to drop this item type outright (measured); it is now a
# tool_call/tool_result pair per changed path, call_id keyed by path rather than list index.
# ---------------------------------------------------------------------------------------------

CODEX_FILE_CHANGE_STARTED_ONE = {
    "type": "item.started",
    "item": {"id": "fc_1", "type": "file_change",
              "changes": [{"path": "a.py", "kind": "update"}], "status": "in_progress"},
}
CODEX_FILE_CHANGE_COMPLETED_ONE = {
    "type": "item.completed",
    "item": {"id": "fc_1", "type": "file_change",
              "changes": [{"path": "a.py", "kind": "update"}], "status": "completed"},
}

CODEX_FILE_CHANGE_STARTED_MULTI = {
    "type": "item.started",
    "item": {"id": "fc_2", "type": "file_change",
              "changes": [{"path": "a.py", "kind": "update"}, {"path": "b.py", "kind": "add"}],
              "status": "in_progress"},
}
CODEX_FILE_CHANGE_COMPLETED_MULTI_REORDERED = {
    "type": "item.completed",
    "item": {"id": "fc_2", "type": "file_change",
              "changes": [{"path": "b.py", "kind": "add"}, {"path": "a.py", "kind": "update"}],
              "status": "completed"},
}

CODEX_FILE_CHANGE_COMPLETED_NO_START = {
    "type": "item.completed",
    "item": {"id": "fc_3", "type": "file_change",
              "changes": [{"path": "c.py", "kind": "delete"}], "status": "completed"},
}

CODEX_FILE_CHANGE_COMPLETED_FAILED = {
    "type": "item.completed",
    "item": {"id": "fc_4", "type": "file_change",
              "changes": [{"path": "d.py", "kind": "update"}], "status": "failed"},
}

CODEX_FILE_CHANGE_NO_CHANGES = {
    "type": "item.completed",
    "item": {"id": "fc_5", "type": "file_change", "changes": [], "status": "completed"},
}


def test_codex_file_change_started_then_completed() -> None:
    _acc, produced = run(
        CodexBackend(), [CODEX_FILE_CHANGE_STARTED_ONE, CODEX_FILE_CHANGE_COMPLETED_ONE]
    )

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["path"] == "a.py"
    assert produced[0]["tool"] == "file_change"
    assert produced[0]["category"] == "edit"
    assert produced[1]["call_id"] == produced[0]["call_id"]
    assert produced[1]["ok"] is True


def test_codex_file_change_multi_change_emits_one_call_per_change() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_FILE_CHANGE_STARTED_MULTI])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_call"]
    assert {event["path"] for event in produced} == {"a.py", "b.py"}
    assert len({event["call_id"] for event in produced}) == 2


def test_codex_file_change_completion_only_item_synthesizes_its_calls() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_FILE_CHANGE_COMPLETED_NO_START])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["path"] == "c.py"
    assert produced[1]["ok"] is True


def test_codex_file_change_repeated_completion_emits_its_call_and_result_once() -> None:
    _acc, produced = run(
        CodexBackend(), [CODEX_FILE_CHANGE_COMPLETED_NO_START, CODEX_FILE_CHANGE_COMPLETED_NO_START]
    )

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]


def test_codex_file_change_reordered_between_start_and_completion_still_pairs_by_path() -> None:
    _acc, produced = run(
        CodexBackend(),
        [CODEX_FILE_CHANGE_STARTED_MULTI, CODEX_FILE_CHANGE_COMPLETED_MULTI_REORDERED],
    )

    assert [event["kind"] for event in produced] == [
        "tool_call", "tool_call", "tool_result", "tool_result",
    ]
    calls_by_path = {event["path"]: event["call_id"] for event in produced if event["kind"] == "tool_call"}
    results = [event for event in produced if event["kind"] == "tool_result"]
    assert {result["call_id"] for result in results} == set(calls_by_path.values())
    assert all(result["ok"] for result in results)


def test_codex_file_change_failed_status_yields_ok_false() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_FILE_CHANGE_COMPLETED_FAILED])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[1]["ok"] is False


def test_codex_file_change_with_no_changes_emits_nothing() -> None:
    _acc, produced = run(CodexBackend(), [CODEX_FILE_CHANGE_NO_CHANGES])

    assert produced == []


# ---------------------------------------------------------------------------------------------
# opencode
# ---------------------------------------------------------------------------------------------

OPENCODE_SESSION = "ses_00b8d2f9affeIu2zpaWJR3voFi"

OPENCODE_TOOL_USE_COMPLETED = {
    "type": "tool_use", "sessionID": OPENCODE_SESSION, "timestamp": 1700000000000,
    "part": {"callID": "call_1", "tool": "write",
             "state": {"status": "completed", "input": {"filePath": "/tmp/x.txt"},
                       "output": "wrote file", "metadata": {"exit": 0}}},
}
OPENCODE_TOOL_USE_ERROR = {
    "type": "tool_use", "sessionID": OPENCODE_SESSION,
    "part": {"callID": "call_2", "tool": "bash",
             "state": {"status": "error", "input": {"command": "false"}, "error": "boom",
                       "metadata": {}}},
}
OPENCODE_TEXT = {
    "type": "text", "sessionID": OPENCODE_SESSION, "part": {"type": "text", "text": "final answer"},
}
OPENCODE_STEP_1 = {
    "type": "step_finish", "sessionID": OPENCODE_SESSION,
    "part": {"reason": "tool-calls", "cost": 0.01, "tokens": {"input": 10, "output": 1}},
}
OPENCODE_STEP_2 = {
    "type": "step_finish", "sessionID": OPENCODE_SESSION,
    "part": {"reason": "stop", "cost": 0.02, "tokens": {"input": 5, "output": 2}},
}


def test_opencode_tool_use_completed_emits_call_then_result() -> None:
    _acc, produced = run(OpencodeBackend(), [OPENCODE_TOOL_USE_COMPLETED])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["category"] == "write"
    assert produced[0]["path"] == "/tmp/x.txt"
    assert produced[1]["ok"] is True
    assert produced[1]["exit_code"] == 0
    assert produced[1]["output_tail"] == "wrote file"
    expected_ts = datetime.fromtimestamp(1700000000000 / 1000, tz=timezone.utc).isoformat()
    assert produced[0]["source_ts"] == expected_ts


def test_opencode_tool_use_error_emits_call_then_failed_result() -> None:
    _acc, produced = run(OpencodeBackend(), [OPENCODE_TOOL_USE_ERROR])

    assert [event["kind"] for event in produced] == ["tool_call", "tool_result"]
    assert produced[0]["category"] == "shell"
    assert produced[0]["command"] == "false"
    assert produced[1]["ok"] is False
    assert produced[1]["output_tail"] == "boom"


def test_opencode_text_is_assistant_text() -> None:
    _acc, produced = run(OpencodeBackend(), [OPENCODE_TEXT])

    assert produced == [{"kind": "assistant_text", "text": "final answer"}]


def test_opencode_step_finish_emits_cumulative_usage_across_steps() -> None:
    acc, produced = run(OpencodeBackend(), [OPENCODE_STEP_1, OPENCODE_STEP_2])

    assert [event["kind"] for event in produced] == ["usage", "usage"]
    assert produced[0]["total_cost_usd"] == pytest.approx(0.01)
    assert produced[1]["total_cost_usd"] == pytest.approx(0.03)
    assert produced[1]["usage"] == {"input": 15, "output": 3}
    assert acc.usage == produced[1]["usage"]


# ---------------------------------------------------------------------------------------------
# vibe — gated on EVERY entry type, not just assistant messages
# ---------------------------------------------------------------------------------------------

VIBE_SESSION = "36e00c2b-1fdf-feed-8748-b9b529f9ecc8"

VIBE_REPLAYED_ASSISTANT = {
    "type": "message", "role": "assistant", "sessionId": VIBE_SESSION, "turnId": None,
    "source": "harness", "createdAt": 1700000000000,
    "content": [{"type": "text", "text": "a prior answer, replayed from history"}],
}
VIBE_REPLAYED_EFFECT = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": None, "createdAt": 1700000000000,
    "id": "eff_replayed", "title": "bash",
    "detail": {"toolName": "bash", "kind": "shell", "input": {"command": "ls"}},
}
VIBE_REPLAYED_CALLBACK = {
    "type": "callback", "sessionId": VIBE_SESSION, "turnId": None, "createdAt": 1700000000000,
    "id": "cb_replayed", "title": "Allow bash?",
    "detail": {"kind": "approval", "effect": {"toolName": "bash", "input": {"command": "ls"}}},
}

VIBE_TURN_A = "turn-a"
VIBE_TURN_START_A = {
    "type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A,
    "source": "turn_start", "createdAt": 1700000001000,
    "content": [{"type": "text", "text": "do the thing"}],
}
VIBE_LIVE_EFFECT_A = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A, "createdAt": 1700000002000,
    "id": "eff_1", "title": "bash",
    "detail": {"toolName": "bash", "kind": "shell", "input": {"command": "ls -la"}},
}

VIBE_TURN_B = "turn-b"
VIBE_TURN_START_B = {
    "type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_B,
    "source": "turn_start", "createdAt": 1700000003000,
    "content": [{"type": "text", "text": "do another thing"}],
}
# Carries turn A's id, arriving after turn B has already started: stale, must produce nothing.
VIBE_STALE_EFFECT = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A, "createdAt": 1700000004000,
    "id": "eff_2", "title": "bash",
    "detail": {"toolName": "bash", "kind": "shell", "input": {"command": "pwd"}},
}

VIBE_BREACH_TURN = "breach-1"
VIBE_BREACH_START = {
    "type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": VIBE_BREACH_TURN,
    "source": "turn_start", "createdAt": 1700000005000,
    "content": [{"type": "text", "text": "read the file"}],
}
VIBE_BREACH_ASSISTANT = {
    "type": "message", "role": "assistant", "sessionId": VIBE_SESSION, "turnId": VIBE_BREACH_TURN,
    "source": None, "createdAt": 1700000006000,
    "content": [{"type": "text", "text": "<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>"}],
}

VIBE_DENY_TURN = "deny-1"
VIBE_DENY_START = {
    "type": "message", "role": "user", "sessionId": VIBE_SESSION, "turnId": VIBE_DENY_TURN,
    "source": "turn_start", "createdAt": 1700000007000,
    "content": [{"type": "text", "text": "run: git commit -am wip"}],
}
VIBE_DENY_CALLBACK = {
    "type": "callback", "sessionId": VIBE_SESSION, "turnId": VIBE_DENY_TURN, "createdAt": 1700000008000,
    "id": "cb_1", "title": "Allow bash?",
    "detail": {"kind": "approval",
               "effect": {"toolName": "bash", "input": {"command": "git commit -am wip"}}},
}


def test_vibe_replayed_assistant_message_before_any_turn_start_is_gated_out() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_REPLAYED_ASSISTANT])

    assert produced == []


def test_vibe_replayed_effect_before_any_turn_start_is_gated_out() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_REPLAYED_EFFECT])

    assert produced == []


def test_vibe_replayed_callback_before_any_turn_start_is_gated_out() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_REPLAYED_CALLBACK])

    assert produced == []


def test_vibe_live_turn_start_user_message_is_reported_as_initial() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_TURN_START_A])

    assert produced == [{"kind": "user_message", "text": "do the thing", "source": "initial",
                          "source_ts": datetime.fromtimestamp(1700000001, tz=timezone.utc).isoformat()}]


def test_vibe_live_effect_is_a_tool_call_with_no_result() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_TURN_START_A, VIBE_LIVE_EFFECT_A])

    assert [event["kind"] for event in produced] == ["user_message", "tool_call"]
    call = produced[1]
    assert call["call_id"] == "eff_1"
    assert call["tool"] == "bash"
    assert call["category"] == "shell"
    assert call["command"] == "ls -la"


def test_vibe_stale_turn_id_effect_after_a_new_turn_start_is_gated_out() -> None:
    _acc, produced = run(
        VibeBackend(), [VIBE_TURN_START_A, VIBE_TURN_START_B, VIBE_STALE_EFFECT]
    )

    assert [event["kind"] for event in produced] == ["user_message", "user_message"]


def test_vibe_stop_event_envelope_is_a_notice_not_assistant_text() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_BREACH_START, VIBE_BREACH_ASSISTANT])

    assert [event["kind"] for event in produced] == ["user_message", "notice"]
    assert produced[1]["text"] == "<vibe_stop_event>Turn limit of 1 reached</vibe_stop_event>"


def test_vibe_approval_callback_is_a_notice() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_DENY_START, VIBE_DENY_CALLBACK])

    assert [event["kind"] for event in produced] == ["user_message", "notice"]
    assert produced[1]["text"] == "auto-denied: git commit -am wip"


def test_vibe_approval_callback_without_a_command_falls_back_to_its_title() -> None:
    callback = {**VIBE_DENY_CALLBACK, "detail": {"kind": "approval", "effect": {"toolName": "bash"}}}
    _acc, produced = run(VibeBackend(), [VIBE_DENY_START, callback])

    assert produced[1]["text"] == "auto-denied: Allow bash?"


# ---------------------------------------------------------------------------------------------
# vibe effect -> tool_result — vibe re-emits the same effect id as it moves from a non-terminal
# status to a terminal one (measured), so the tool_call fires once (already covered above by
# test_vibe_live_effect_is_a_tool_call_with_no_result, where the effect carries no "state" at
# all) and the tool_result fires once, exactly when state.status first reads terminal.
# ---------------------------------------------------------------------------------------------

VIBE_EFFECT_EDIT_IN_PROGRESS = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A, "createdAt": 1700000002000,
    "id": "eff_edit", "title": "edit_file",
    "detail": {"toolName": "edit_file", "kind": "file_edit",
               "input": {"filePath": "a.py", "oldString": "foo", "newString": "bar"}},
    "state": {"status": "in_progress"},
}
VIBE_EFFECT_EDIT_COMPLETED = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A, "createdAt": 1700000002500,
    "id": "eff_edit", "title": "edit_file",
    "detail": {"toolName": "edit_file", "kind": "file_edit",
               "input": {"filePath": "a.py", "oldString": "foo", "newString": "bar"}},
    "state": {"status": "completed", "output": {"filePath": "a.py"}},
}
VIBE_EFFECT_EDIT_FAILED_FIRST_SIGHT = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A, "createdAt": 1700000002000,
    "id": "eff_failed", "title": "edit_file",
    "detail": {"toolName": "edit_file", "kind": "file_edit",
               "input": {"filePath": "b.py", "oldString": "foo", "newString": "bar"}},
    "state": {"status": "failed", "error": "could not apply patch"},
}
VIBE_EFFECT_WRITE_CANCELLED = {
    "type": "effect", "sessionId": VIBE_SESSION, "turnId": VIBE_TURN_A, "createdAt": 1700000002000,
    "id": "eff_cancelled", "title": "write_file",
    "detail": {"toolName": "write_file", "kind": "file_write", "input": {"filePath": "c.py"}},
    "state": {"status": "cancelled"},
}


def test_vibe_effect_non_terminal_then_terminal_yields_exactly_one_call_and_one_result() -> None:
    _acc, produced = run(
        VibeBackend(), [VIBE_TURN_START_A, VIBE_EFFECT_EDIT_IN_PROGRESS, VIBE_EFFECT_EDIT_COMPLETED]
    )

    kinds = [event["kind"] for event in produced]
    assert kinds.count("tool_call") == 1
    assert kinds.count("tool_result") == 1
    call = next(event for event in produced if event["kind"] == "tool_call")
    result = next(event for event in produced if event["kind"] == "tool_result")
    assert call["call_id"] == "eff_edit" == result["call_id"]
    assert call["path"] == "a.py"
    assert result["ok"] is True


def test_vibe_effect_terminal_on_first_sight_yields_call_then_result() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_TURN_START_A, VIBE_EFFECT_EDIT_FAILED_FIRST_SIGHT])

    kinds = [event["kind"] for event in produced]
    assert kinds[-2:] == ["tool_call", "tool_result"]
    result = produced[-1]
    assert result["call_id"] == "eff_failed"
    assert result["ok"] is False


def test_vibe_effect_cancelled_status_yields_ok_false() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_TURN_START_A, VIBE_EFFECT_WRITE_CANCELLED])

    result = next(event for event in produced if event["kind"] == "tool_result")
    assert result["ok"] is False


def test_vibe_effect_non_terminal_status_yields_no_result() -> None:
    _acc, produced = run(VibeBackend(), [VIBE_TURN_START_A, VIBE_EFFECT_EDIT_IN_PROGRESS])

    assert [event["kind"] for event in produced] == ["user_message", "tool_call"]


# ---------------------------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------------------------


def test_truncate_head_never_splits_a_multibyte_char_and_respects_the_byte_limit() -> None:
    text = "測" * 2000  # 3 bytes each in UTF-8
    result = truncate_head(text, 10)

    encoded = result.encode("utf-8")
    assert len(encoded) <= 10
    assert set(result) <= {"測"}
    # decode() with errors="ignore" would silently keep a truncated multi-byte sequence's leading
    # bytes as replacement noise if the slicing were wrong; asserting every char round-trips
    # cleanly is what rules that out.
    assert result.encode("utf-8").decode("utf-8") == result


def test_truncate_tail_never_splits_a_multibyte_char_and_keeps_the_end() -> None:
    text = "a" * 5000 + "測" * 10
    result = truncate_tail(text, 10)

    assert len(result.encode("utf-8")) <= 10
    assert result != "" and set(result) <= {"測"}


def test_truncate_head_and_tail_are_no_ops_under_the_limit() -> None:
    assert truncate_head("short", 100) == "short"
    assert truncate_tail("short", 100) == "short"


def test_preview_caps_at_input_preview_bytes() -> None:
    huge = {"data": "x" * 10000}
    text = preview(huge)

    assert len(text.encode("utf-8")) <= INPUT_PREVIEW_BYTES


def test_preview_leaves_a_str_input_as_is_up_to_the_limit() -> None:
    assert preview("plain text") == "plain text"


def test_tool_call_edit_truncates_old_and_new_independently() -> None:
    call = tool_call(
        call_id="c", tool="Edit", category="edit", input={},
        edit=("o" * 20000, "n" * 20000),
    )

    assert len(call["edit"]["old"].encode("utf-8")) <= EDIT_TEXT_BYTES
    assert len(call["edit"]["new"].encode("utf-8")) <= EDIT_TEXT_BYTES


def test_tool_call_unknown_category_falls_back_to_other() -> None:
    call = tool_call(call_id="c", tool="t", category="not-a-real-category", input=None)

    assert call["category"] == "other"
    assert "other" in CATEGORIES


def test_tool_result_output_tail_caps_and_keeps_the_end() -> None:
    huge_output = "x" * 1000 + "y" * 10000
    result = tool_result(call_id="c", ok=True, output=huge_output)

    assert len(result["output_tail"].encode("utf-8")) <= OUTPUT_TAIL_BYTES
    assert result["output_tail"].endswith("y")
    assert "x" not in result["output_tail"]


def test_tool_result_non_str_output_is_json_rendered_then_tail_truncated() -> None:
    result = tool_result(call_id="c", ok=False, output={"detail": "boom", "code": 7})

    assert result["output_tail"] == '{"code": 7, "detail": "boom"}'


def test_iso_from_epoch_ms_rejects_bool_and_non_finite_values() -> None:
    assert iso_from_epoch_ms(True) is None
    assert iso_from_epoch_ms(False) is None
    assert iso_from_epoch_ms(float("nan")) is None
    assert iso_from_epoch_ms(float("inf")) is None
    assert iso_from_epoch_ms(-1) is None
    assert iso_from_epoch_ms("not a number") is None
    assert iso_from_epoch_ms(0) == datetime.fromtimestamp(0, tz=timezone.utc).isoformat()


def test_iso_string_passes_through_non_empty_str_only() -> None:
    assert iso_string("2026-09-24T10:00:00.000Z") == "2026-09-24T10:00:00.000Z"
    assert iso_string("") is None
    assert iso_string(None) is None
    assert iso_string(12345) is None


# ---------------------------------------------------------------------------------------------
# cross-backend invariants
# ---------------------------------------------------------------------------------------------


def test_every_registered_backend_has_a_callable_normalize_method() -> None:
    for backend in backends.BACKENDS.values():
        assert callable(getattr(backend, "normalize", None))
        assert isinstance(backend, backends.Backend)
