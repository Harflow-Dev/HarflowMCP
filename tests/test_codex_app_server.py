"""Codex caller approvals through `codex app-server`: the state machine, admission, encoding and the pre-spawn refusals.

Unit level, no process: frames are fed to the same functions the generic approval runtime calls. The
end-to-end runs against the scripted fake binary are in test_codex_app_server_runs.py.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import approval_runtime, approvals, backends
from harflow_mcp.backends import Accumulator, Invocation, get
from harflow_mcp.backends import codex_app_server as app
from harflow_mcp.backends import codex_qualification as qualification
from harflow_mcp.backends.base import ApprovalRefused, UnsupportedCapability
from harflow_mcp.backends.codex import CodexBackend, UnsafeInvocationError
from test_caller_write_approvals import CONTEXT, Registry, runtime, runtime_task, tmp_path  # noqa: F401

FIXTURES = Path(__file__).parent / "fixtures" / "codex_app_server"
VERSION = "0.163.0"
THREAD, TURN = "thread-1", "turn-1"


def make_ctx(cwd, **overrides):
    cwd = os.path.realpath(cwd)
    ctx = {"transport": app.TRANSPORT, "version": VERSION, "cwd": cwd, "git_root": cwd, "freedom": "write_in_repo", "network": False,
           "model": None, "effort": None, "prompt": "do the thing", "thread_id": None}
    return {**ctx, **overrides}


@pytest.fixture
def cwd(tmp_path_factory):
    return os.path.realpath(tmp_path_factory.mktemp("repo"))


def thread_result(ctx, **over):
    result = {"thread": {"id": ctx.get("thread_id") or THREAD, "cliVersion": VERSION}, "approvalPolicy": "on-request", "approvalsReviewer": "user",
              "cwd": ctx["cwd"], "runtimeWorkspaceRoots": [ctx["cwd"]],
              "sandbox": {"type": "workspaceWrite", "writableRoots": [], "networkAccess": ctx["network"]}}
    for key, value in over.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = {**result[key], **value}
        else:
            result[key] = value
    return result


def hooks_ok(ctx, **over):
    return {"data": [{"cwd": ctx["cwd"], "hooks": [], "warnings": [], "errors": [], **over}]}


def answer(state, method, result=None, error=None):
    key = next(k for k, m in state.pending.items() if m == method)
    frame = {"id": app.untyped(key)}
    frame.update({"error": error} if error is not None else {"result": result})
    app.step(state, frame)


def started(ctx, **thread_over):
    """A state driven through initialize, thread, hooks and turn/start: the point at which requests are admissible."""
    state = app.State(ctx)
    acc = SimpleNamespace(stream_state={"control_context": ctx, "app_server": state})
    assert [json.loads(f)["method"] for f in app.initial_frames(acc)] == ["initialize"]
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (Mac OS) fake"})
    answer(state, "thread/resume" if ctx.get("thread_id") else "thread/start", thread_result(ctx, **thread_over))
    answer(state, "hooks/list", hooks_ok(ctx))
    answer(state, "turn/start", {"turn": {"id": TURN, "status": "inProgress"}})
    state.outbox.clear()
    return state


def command(item="call-1", rid=0, **over):
    params = {"threadId": state_thread(), "turnId": TURN, "itemId": item, "startedAtMs": 5, "kind": "command", "command": "/bin/zsh -lc 'touch x'",
              "cwd": "/repo", "commandActions": [{"type": "unknown", "command": "touch x"}], "availableDecisions": ["accept", "cancel"]}
    params.update(over)
    return {"id": rid, "method": app.COMMAND_REQUEST, "params": {k: v for k, v in params.items() if v is not ...}}


def state_thread():
    return THREAD


# --- pins, argv and the outgoing allowlist ------------------------------------------------------------------

def test_argv_carries_the_complete_pin_set_and_no_trust_pin(cwd):
    argv = app.argv(make_ctx(cwd, effort="high"))
    assert argv[:2] == ["codex", "app-server"] and argv[-2:] == ["--listen", "stdio://"]
    pairs = [argv[i + 1] for i, token in enumerate(argv) if token == "-c"]
    assert pairs == ['approval_policy="on-request"', 'approvals_reviewer="user"', 'sandbox_mode="workspace-write"', "sandbox_workspace_write.writable_roots=[]",
                     "sandbox_workspace_write.network_access=false", "features.hooks=false", "features.request_permissions_tool=false",
                     "features.exec_permission_approvals=false", 'shell_environment_policy.inherit="core"', 'model_reasoning_effort="high"']
    assert not any(pair.startswith("projects") for pair in pairs)
    assert "network_access=true" in " ".join(app.argv(make_ctx(cwd, network=True)))


def test_config_map_repeats_the_pins_and_overrides_project_trust(cwd):
    ctx = make_ctx(cwd, network=True)
    mapped = app.config_map(ctx)
    assert mapped["approval_policy"] == "on-request" and mapped["approvals_reviewer"] == "user" and mapped["sandbox_mode"] == "workspace-write"
    assert mapped["sandbox_workspace_write"] == {"writable_roots": [], "network_access": True}
    assert mapped["features"] == {"hooks": False, "request_permissions_tool": False, "exec_permission_approvals": False}
    assert mapped["shell_environment_policy"] == {"inherit": "core"}
    assert mapped["projects"] == {ctx["git_root"]: {"trust_level": "untrusted"}}


def test_every_outgoing_frame_obeys_its_field_allowlist(cwd):
    ctx = make_ctx(cwd, model="gpt-x", thread_id=None)
    state = app.State(ctx)
    frames = [json.loads(f) for f in app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))]
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
    answer(state, "thread/start", thread_result(ctx))
    answer(state, "hooks/list", hooks_ok(ctx))
    frames += [json.loads(f) for f in state.outbox]
    methods = [f["method"] for f in frames]
    assert methods == ["initialize", "initialized", "thread/start", "hooks/list", "turn/start"]
    for frame in frames:
        assert set(frame.get("params", {})) <= app.ALLOWED_PARAMS.get(frame["method"], frozenset())
    turn = frames[-1]["params"]
    assert set(turn) == {"threadId", "input"} and turn["input"] == [{"type": "text", "text": "do the thing"}]
    thread = frames[2]["params"]
    assert thread["approvalPolicy"] == "on-request" and thread["approvalsReviewer"] == "user" and thread["sandbox"] == "workspace-write" and thread["model"] == "gpt-x"
    assert [frame["id"] for frame in frames if "id" in frame] == [1, 2, 3, 4]


@pytest.mark.parametrize("field", sorted(app.TURN_FORBIDDEN))
def test_turn_start_can_never_carry_a_policy_override(cwd, field):
    state = app.State(make_ctx(cwd))
    with pytest.raises(ValueError, match="allowlist"):
        app.request_frame(state, "turn/start", {"threadId": "t", "input": [], field: "never"})
    with pytest.raises(ValueError, match="allowlist"):
        app.request_frame(state, "thread/start", {"cwd": "/x", "sandboxPolicy": {}})
    with pytest.raises(ValueError, match="allowlist"):
        app.request_frame(state, "turn/interrupt", {})


def test_resume_sends_the_thread_id_and_the_same_pins(cwd):
    ctx = make_ctx(cwd, thread_id="old-thread")
    state = app.State(ctx)
    app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
    frames = [json.loads(f) for f in state.outbox]
    assert frames[1]["method"] == "thread/resume" and frames[1]["params"]["threadId"] == "old-thread"
    assert frames[1]["params"]["config"]["projects"] == {ctx["git_root"]: {"trust_level": "untrusted"}}


def test_typed_keys_keep_integer_and_string_ids_apart():
    assert app.typed_key(0) == "i:0" and app.typed_key("0") == "s:0" and app.typed_key(True) is None and app.typed_key(1.5) is None and app.typed_key(None) is None
    assert app.typed_key("") is None and app.typed_key("x" * 257) is None
    assert app.untyped("i:7") == 7 and type(app.untyped("i:7")) is int and app.untyped("s:7") == "7"


# --- verification -------------------------------------------------------------------------------------------

def test_a_wrong_or_missing_user_agent_version_refuses_before_anything_is_sent(cwd):
    for agent in (f"harflow/0.162.0 (x)", "no-version", None):
        state = app.State(make_ctx(cwd))
        app.initial_frames(SimpleNamespace(stream_state={"control_context": make_ctx(cwd), "app_server": state}))
        with pytest.raises(ApprovalRefused, match="qualified on 0.163.0"):
            answer(state, "initialize", {"userAgent": agent})
        assert state.outbox == [] and state.failed


THREAD_MISMATCHES = {
    "cliVersion": {"thread": {"id": THREAD, "cliVersion": "0.164.0"}},
    "approvalPolicy": {"approvalPolicy": "never"},
    "approvalsReviewer": {"approvalsReviewer": "auto_review"},
    "sandbox type": {"sandbox": {"type": "dangerFullAccess"}},
    "writableRoots": {"sandbox": {"writableRoots": ["/elsewhere"]}},
    "networkAccess": {"sandbox": {"networkAccess": True}},
    "runtimeWorkspaceRoots": {"runtimeWorkspaceRoots": ["/elsewhere"]},
    "extra root": {"runtimeWorkspaceRoots": ["__cwd__", "/extra"]},
    "cwd": {"cwd": "/elsewhere"},
}


@pytest.mark.parametrize("resumed", [False, True])
@pytest.mark.parametrize("name", sorted(THREAD_MISMATCHES))
def test_thread_response_mismatches_refuse_on_start_and_resume(cwd, name, resumed):
    ctx = make_ctx(cwd, thread_id="old-thread" if resumed else None)
    over = json.loads(json.dumps(THREAD_MISMATCHES[name]).replace("__cwd__", ctx["cwd"]))
    state = app.State(ctx)
    app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
    with pytest.raises(ApprovalRefused, match="verification failed"):
        answer(state, "thread/resume" if resumed else "thread/start", thread_result(ctx, **over))
    assert not state.verified and state.turn_id is None


def test_a_resumed_thread_must_be_the_requested_one(cwd):
    ctx = make_ctx(cwd, thread_id="old-thread")
    state = app.State(ctx)
    app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
    with pytest.raises(ApprovalRefused, match="resumed thread id"):
        answer(state, "thread/resume", thread_result(ctx, thread={"id": "another", "cliVersion": VERSION}))


def _hooks_cases():
    cwd = "__cwd__"
    good = {"cwd": cwd, "hooks": [], "warnings": [], "errors": []}
    return {
        "hooks reported": {"data": [{**good, "hooks": [{"key": "k"}]}]},
        "errors reported": {"data": [{**good, "errors": [{"message": "m", "path": "p"}]}]},
        "empty data": {"data": []},
        "no data": {},
        "unrelated cwd only": {"data": [{**good, "cwd": "/somewhere/else"}]},
        "hooks in an unrelated entry": {"data": [good, {**good, "cwd": "/other", "hooks": [{"key": "k"}]}]},
        "missing cwd": {"data": [{k: v for k, v in good.items() if k != "cwd"}]},
        "missing hooks": {"data": [{k: v for k, v in good.items() if k != "hooks"}]},
        "missing errors": {"data": [{k: v for k, v in good.items() if k != "errors"}]},
        "missing warnings": {"data": [{k: v for k, v in good.items() if k != "warnings"}]},
        "wrong types": {"data": [{**good, "hooks": "none"}]},
        "not a list": {"data": "x"},
        "entry not an object": {"data": [3]},
    }


@pytest.mark.parametrize("name", sorted(_hooks_cases()))
def test_hooks_list_must_cover_the_cwd_be_complete_and_report_no_hooks_or_errors(cwd, name):
    ctx = make_ctx(cwd)
    result = json.loads(json.dumps(_hooks_cases()[name]).replace("__cwd__", ctx["cwd"]))
    state = app.State(ctx)
    app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
    answer(state, "thread/start", thread_result(ctx))
    with pytest.raises(ApprovalRefused):
        answer(state, "hooks/list", result)
    assert not state.verified and not any(json.loads(f).get("method") == "turn/start" for f in state.outbox)


def test_a_hooks_list_with_an_extra_unrelated_clean_entry_still_passes(cwd):
    ctx = make_ctx(cwd)
    state = app.State(ctx)
    app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
    answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
    answer(state, "thread/start", thread_result(ctx))
    answer(state, "hooks/list", {"data": [hooks_ok(ctx)["data"][0], {"cwd": "/other", "hooks": [], "warnings": ["w"], "errors": []}]})
    assert state.verified


def test_json_rpc_errors_on_harflow_requests_stop_the_run_with_the_structured_error(cwd):
    for method in ("initialize", "thread/start", "hooks/list", "turn/start"):
        ctx = make_ctx(cwd)
        state = app.State(ctx)
        app.initial_frames(SimpleNamespace(stream_state={"control_context": ctx, "app_server": state}))
        if method != "initialize":
            answer(state, "initialize", {"userAgent": f"harflow/{VERSION} (x)"})
        if method in {"hooks/list", "turn/start"}:
            answer(state, "thread/start", thread_result(ctx))
        if method == "turn/start":
            answer(state, "hooks/list", hooks_ok(ctx))
        with pytest.raises(ApprovalRefused, match=f"error for {method}: code -32000: boom"):
            answer(state, method, error={"code": -32000, "message": "boom"})


def test_a_frame_is_a_response_only_if_it_matches_an_outstanding_harflow_id(cwd):
    state = started(make_ctx(cwd))
    app.step(state, {"id": 0, "result": {"turn": {"id": "other"}}})
    app.step(state, {"id": "i:1", "result": {}})
    assert state.turn_id == TURN and state.failed is None


@pytest.mark.parametrize("method", ["hook/started", "hook/completed", "item/autoApprovalReview/started", "item/autoApprovalReview/completed",
                                    "guardianWarning", "autoApprovalReview/strictReviewRequired"])
def test_runtime_tripwires_stop_the_run(cwd, method):
    state = started(make_ctx(cwd))
    with pytest.raises(ApprovalRefused, match=method):
        app.step(state, {"method": method, "params": {}})
    assert state.failed


@pytest.mark.parametrize("method", ["item/started", "item/completed", "turn/started", "turn/completed"])
def test_items_and_turns_before_verification_are_refused(cwd, method):
    ctx = make_ctx(cwd)
    state = app.State(ctx)
    with pytest.raises(ApprovalRefused, match="before the version, thread and policy checks passed"):
        app.step(state, {"method": method, "params": {}})


def test_turn_completed_is_terminal_only_for_the_active_thread_and_turn(cwd):
    state = started(make_ctx(cwd))
    app.step(state, {"method": "turn/completed", "params": {"threadId": "other", "turn": {"id": TURN, "status": "completed"}}})
    app.step(state, {"method": "turn/completed", "params": {"threadId": THREAD, "turn": {"id": "old", "status": "completed"}}})
    assert state.terminal is None and not app.finished(SimpleNamespace(stream_state={"app_server": state}))
    app.step(state, {"method": "turn/completed", "params": {"threadId": THREAD, "turn": {"id": TURN, "status": "interrupted"}}})
    assert state.terminal == {"status": "interrupted", "error": None} and app.finished(SimpleNamespace(stream_state={"app_server": state}))


# --- admission ---------------------------------------------------------------------------------------------

def qual(state):
    return {"version": VERSION, "observed": VERSION, "write": False, "state": state}


def test_a_request_before_verification_stops_the_run(cwd):
    state = app.State(make_ctx(cwd))
    with pytest.raises(ApprovalRefused, match="before the version"):
        app.decode(command(), qual(state))
    with pytest.raises(ApprovalRefused):
        app.decode(command(), None)


def test_a_valid_command_request_is_admitted_with_every_param_but_started_at_in_the_digest(cwd):
    state = started(make_ctx(cwd))
    decoded = app.decode(command(proposedExecpolicyAmendment=["touch", "x"], reason="why"), qual(state))
    assert "system_rejection" not in decoded
    assert decoded["native_request_id"] == "i:0" and decoded["tool_use_id"] == "call-1" and decoded["native_method"] == app.COMMAND_REQUEST
    assert decoded["decision_reason"] == "why" and decoded["decision_reason_type"] == "command" and decoded["may_run_outside_sandbox"] is True
    inputs = decoded["action"]["input"]
    assert "startedAtMs" not in inputs and inputs["proposedExecpolicyAmendment"] == ["touch", "x"] and inputs["availableDecisions"] == ["accept", "cancel"]
    assert decoded["action"]["tool_name"] == "commandExecution"
    again = app.decode(command(item="call-2", rid=1, startedAtMs=99, proposedExecpolicyAmendment=["touch", "x"], reason="why"), qual(state))
    assert approvals.digest({**decoded["action"], "input": {**inputs, "itemId": "x"}}) == approvals.digest({**again["action"], "input": {**again["action"]["input"], "itemId": "x"}})


SYSTEM = {
    "wrong thread": (dict(threadId="other"), "not_active_turn"),
    "historical turn": (dict(turnId="old-turn"), "not_active_turn"),
    "write stdin": (dict(kind="writeStdin", approvalId="ws1"), "unsupported_kind"),
    "other kind": (dict(kind="surprise"), "unsupported_kind"),
    "absent kind": (dict(kind=...), "unsupported_kind"),
    "null command": (dict(command=None), "missing_command"),
    "absent command": (dict(command=...), "missing_command"),
    "blank command": (dict(command="  "), "missing_command"),
    "additional permissions": (dict(additionalPermissions={"fileSystem": {"write": ["/x"]}}), "additional_permissions"),
    "network context": (dict(networkApprovalContext={"host": "x"}), "network_context"),
    "network amendments": (dict(proposedNetworkPolicyAmendments=[{"host": "x"}]), "network_context"),
}


@pytest.mark.parametrize("name", sorted(SYSTEM))
def test_unqualified_command_requests_become_harflow_rejections(cwd, name):
    over, reason = SYSTEM[name]
    state = started(make_ctx(cwd))
    decoded = app.decode(command(**over), qual(state))
    assert decoded["system_rejection"] == reason
    row = {**decoded, "native_request_id": "i:0"}
    assert json.loads(app.encode(row, "reject")) == {"id": 0, "result": {"decision": "decline"}}
    with pytest.raises(ValueError):
        app.encode(row, "approve")


@pytest.mark.parametrize("decisions", [..., None, [], ["accept"], ["cancel"], ["decline"], ["accept", "decline"], "accept", [{"acceptWithExecpolicyAmendment": {}}, "cancel"]])
def test_a_command_request_without_accept_and_cancel_stops_the_run(cwd, decisions):
    state = started(make_ctx(cwd))
    with pytest.raises(ApprovalRefused, match="accept and cancel"):
        app.decode(command(availableDecisions=decisions), qual(state))


def test_decline_need_not_be_listed_and_extra_decisions_are_never_used(cwd):
    state = started(make_ctx(cwd))
    decisions = ["accept", {"acceptWithExecpolicyAmendment": {"execpolicy_amendment": ["touch"]}}, "cancel"]
    decoded = app.decode(command(availableDecisions=decisions), qual(state))
    for decision in ("approve", "reject"):
        result = json.loads(app.encode({**decoded}, decision))["result"]["decision"]
        assert result in {"accept", "decline"}


@pytest.mark.parametrize("method,reason", [
    ("item/permissions/requestApproval", "unsupported_request"), ("mcpServer/elicitation/request", "unsupported_request"),
    ("item/tool/requestUserInput", "unsupported_request"), ("item/tool/call", "unsupported_request"), ("applyPatchApproval", "unsupported_request"),
    ("execCommandApproval", "unsupported_request"), ("account/chatgptAuthTokens/refresh", "unsupported_request"), ("some/future/request", "unsupported_request"),
    (app.FILE_CHANGE_REQUEST, "file_change_unsupported"),
])
def test_every_other_server_request_is_answered_and_recorded_as_a_system_rejection(cwd, method, reason):
    state = started(make_ctx(cwd))
    event = {"id": "abc", "method": method, "params": {"itemId": "p1", "threadId": THREAD, "turnId": TURN}}
    decoded = app.decode(event, qual(state))
    assert decoded["system_rejection"] == reason and decoded["native_request_id"] == "s:abc"
    frame = json.loads(app.encode(decoded, "reject"))
    assert frame["id"] == "abc"
    if method == app.FILE_CHANGE_REQUEST:
        assert frame["result"] == {"decision": "decline"} and decoded["tool_use_id"] == "fileChange:p1"
    else:
        assert frame["error"]["code"] == -32000 and "Harflow refused" in frame["error"]["message"] and "result" not in frame


def test_a_mcp_elicitation_without_item_id_and_a_string_id_are_handled(cwd):
    state = started(make_ctx(cwd))
    event = {"id": 7, "method": "mcpServer/elicitation/request", "params": {"threadId": THREAD, "turnId": TURN, "serverName": "x", "message": "m"}}
    decoded = app.decode(event, qual(state))
    assert decoded["tool_use_id"] == "mcpServer/elicitation/request:i:7" and json.loads(app.encode(decoded, "reject"))["id"] == 7


def test_requests_without_a_usable_id_or_params_are_uncorrelated(cwd):
    state = started(make_ctx(cwd))
    for event in ({"id": 1.5, "method": app.COMMAND_REQUEST, "params": {}}, {"id": True, "method": app.COMMAND_REQUEST, "params": {}},
                  {"id": 1, "method": app.COMMAND_REQUEST, "params": "x"}, {"id": 1, "method": app.COMMAND_REQUEST, "params": {"itemId": ""}}):
        with pytest.raises(ValueError):
            app.decode(event, qual(state))


def test_encode_echoes_the_original_id_type_and_expiry_is_a_decline(cwd):
    state = started(make_ctx(cwd))
    integer = app.decode(command(rid=0), qual(state))
    string = app.decode(command(item="call-2", rid="0"), qual(state))
    assert json.loads(app.encode(integer, "approve")) == {"id": 0, "result": {"decision": "accept"}}
    assert json.loads(app.encode(string, "approve")) == {"id": "0", "result": {"decision": "accept"}}
    assert json.loads(app.encode({**integer, "expiry_rejection": True}, "reject")) == {"id": 0, "result": {"decision": "decline"}}
    with pytest.raises(ValueError):
        app.encode({**integer, "expiry_rejection": True}, "approve")
    with pytest.raises(ValueError):
        app.encode(integer, "maybe")
    assert b"acceptForSession" not in app.encode(integer, "approve") and b"cancel" not in app.encode(integer, "reject")


def test_oversize_requests_become_a_bounded_rejection_row(cwd):
    state = started(make_ctx(cwd))
    decoded = app.decode_oversize(command(command="x" * 100), qual(state))
    assert decoded["system_rejection"] == "request_too_large" and decoded["action"]["input"] == {} and decoded["tool_use_id"] == "call-1"
    assert json.loads(app.encode({**decoded}, "reject"))["result"] == {"decision": "decline"}
    assert app.decode_oversize(command(), qual(app.State(make_ctx(cwd)))) is None


def test_frame_kinds_and_keys():
    assert app.frame_kind(command()) == "request" and app.request_key(command(rid="a")) == "s:a"
    assert app.frame_kind({"method": "serverRequest/resolved", "params": {"threadId": "t", "requestId": 4}}) == "resolved"
    assert app.request_key({"method": "serverRequest/resolved", "params": {"requestId": 4}}) == "i:4" and app.request_key({"method": "serverRequest/resolved", "params": {"requestId": "4"}}) == "s:4"
    for event in ({"id": 1, "result": {}}, {"method": "item/started", "params": {}}, {"type": "assistant"}, {}):
        assert app.frame_kind(event) is None


def test_outcomes_need_the_active_thread_turn_and_a_unique_admitted_item(cwd):
    state = started(make_ctx(cwd))
    app.decode(command(item="call-1"), qual(state))
    done = lambda **over: {"method": "item/completed", "params": {"threadId": THREAD, "turnId": TURN, "item": {"type": "commandExecution", "id": "call-1", "status": "completed", "exitCode": 0, **over}}}
    assert app.outcomes(done(), state) == [("call-1", "succeeded")]
    assert app.outcomes(done(exitCode=2), state) == [("call-1", "failed")]
    assert app.outcomes(done(status="declined", exitCode=None), state) == [("call-1", "failed")]
    assert app.outcomes(done(id="never-admitted"), state) == []
    assert app.outcomes({**done(), "params": {**done()["params"], "turnId": "old"}}, state) == []
    assert app.outcomes({**done(), "params": {**done()["params"], "threadId": "other"}}, state) == []
    assert app.outcomes(done(type="fileChange"), state) == [] and app.outcomes(done(), None) == []
    app.decode(command(item="call-1", rid=9), qual(state))
    assert app.outcomes(done(), state) == []


# --- translation, classification and recorded transcripts ------------------------------------------------------

def transcript_frames(name):
    rows = [json.loads(line) for line in (FIXTURES / "transcripts" / f"{name}.jsonl").read_text().splitlines()]
    return [row["frame"] for row in rows if row["dir"] == "in" and isinstance(row["frame"], dict)]


def test_recorded_frames_translate_to_the_events_normalize_already_understands():
    backend = get("codex")
    acc = Accumulator()
    normalized = []
    for frame in transcript_frames("a1-basic"):
        backend.ingest(frame, acc)
        normalized += backend.normalize(frame, acc)
    kinds = [event["kind"] for event in normalized]
    assert "tool_call" in kinds and "tool_result" in kinds and "assistant_text" in kinds
    assert acc.session_id and acc.saw_final_message and acc.summary == "done" and acc.stream_state["app_server_turn_status"] == "completed"
    assert backend.classify(acc, 0) == "completed"
    declined = [e for e in normalized if e["kind"] == "tool_result" and e.get("ok") is False]
    assert declined, "the declined escalated command is a failed tool result"


@pytest.mark.parametrize("name,status", [("f1-response-failed", "failed"), ("a2-on-request-workspace-write-cancel", "interrupted"), ("f1-http-400", "failed")])
def test_failed_and_interrupted_turns_never_classify_as_success(name, status):
    backend = get("codex")
    acc = Accumulator()
    for frame in transcript_frames(name):
        backend.ingest(frame, acc)
        backend.normalize(frame, acc)
    assert acc.stream_state["app_server_turn_status"] == "failed"
    assert backend.classify(acc, 0) == "failed"


def test_an_agent_message_followed_by_a_failed_turn_fails_and_a_missing_terminal_fails():
    backend = get("codex")
    acc = Accumulator()
    backend.ingest({"method": "item/completed", "params": {"item": {"type": "agentMessage", "id": "m", "text": "all done"}}}, acc)
    assert acc.saw_final_message and backend.classify(acc, 0) == "failed"  # no terminal turn evidence at all
    backend.ingest({"method": "turn/completed", "params": {"turn": {"id": "t", "status": "failed", "error": {"message": "boom"}}}}, acc)
    assert backend.classify(acc, 0) == "failed" and acc.is_error
    clean = Accumulator()
    backend.ingest({"method": "item/completed", "params": {"item": {"type": "agentMessage", "id": "m", "text": "ok"}}}, clean)
    backend.ingest({"method": "turn/completed", "params": {"turn": {"id": "t", "status": "completed"}}}, clean)
    assert backend.classify(clean, 0) == "completed" and backend.classify(clean, 1) == "failed" and backend.classify(clean, None) == "failed"


def test_exec_events_ingest_exactly_as_before_and_exec_classification_ignores_the_gate():
    backend = get("codex")
    acc = Accumulator()
    for event in ({"type": "thread.started", "thread_id": "t1"}, {"type": "item.completed", "item": {"type": "agent_message", "text": "ok"}}, {"type": "turn.completed", "usage": {}}):
        backend.ingest(event, acc)
    assert acc.session_id == "t1" and backend.classify(acc, 0) == "completed" and "app_server_seen" not in acc.stream_state


def test_the_session_id_is_disclosed_only_after_the_thread_verified(cwd):
    backend = get("codex")
    ctx = make_ctx(cwd)
    state = app.State(ctx)
    acc = Accumulator()
    acc.stream_state.update(control_context=ctx, app_server=state)
    frame = {"id": 2, "result": thread_result(ctx)}
    backend.ingest(frame, acc)
    assert acc.session_id is None
    state.thread_verified = True
    state.thread_id = "some-other-thread"
    backend.ingest(frame, acc)
    assert acc.session_id is None, "a thread response for another thread cannot change the session"
    state.thread_id = THREAD
    backend.ingest(frame, acc)
    assert acc.session_id == THREAD


# --- qualification -------------------------------------------------------------------------------------------

def test_the_qualification_table_records_the_facts_the_design_depends_on():
    row = qualification.MEASURED["0.163.0"]
    assert row["platforms"] == ("darwin",) and row["freedoms"] == ("write_in_repo", "publish") and row["caller_approval_tools"] == ("commandExecution",)
    assert {"no_mid_run_config_reload", "argv_hooks_pin_reflected_by_hooks_list", "trust_map_override_writes_nothing", "decline_accepted_though_unlisted",
            "explicit_pins_override_config", "turn_start_overrides_replace_pins"} <= {k for k, v in row["facts"].items() if v}
    assert qualification.parse_version("codex-cli 0.163.0") == "0.163.0" and qualification.parse_version("codex-cli 0.163.0-alpha.1") is None
    assert qualification.parse_user_agent("harflow/0.163.0 (Mac OS 26; arm64) iTerm (x; 0)") == "0.163.0" and qualification.parse_user_agent("junk") is None


def test_qualify_requires_a_measured_version_on_a_measured_platform(monkeypatch):
    backend = get("codex")
    monkeypatch.setattr(backends, "version", lambda b: "codex-cli 0.163.0")
    assert qualification.qualify(backend, "darwin") == "0.163.0"
    with pytest.raises(UnsupportedCapability, match="measured only on darwin"):
        qualification.qualify(backend, "linux")
    for observed in ("codex-cli 0.162.0", "codex-cli 0.164.0", None, "weird"):
        monkeypatch.setattr(backends, "version", lambda b, o=observed: o)
        with pytest.raises(UnsupportedCapability, match="not qualified"):
            qualification.qualify(backend, "darwin")


def test_codex_reports_caller_approvals_as_qualified_but_not_for_workflows_or_writes():
    backend = get("codex")
    assert backend.capabilities.supports_caller_approvals is True
    support = backend.caller_approval_support()
    assert support["supported"] is True and support["versions"] == ["0.163.0"] and support["platforms"] == ["darwin"] and support["write_approvals"] is False and support["workflows"] is False
    assert backends.capability_metadata(backend)["caller_approval_support"]["transport"] == "codex app-server"
    assert backend.workflow_caller_approvals_supported() is False
    with pytest.raises(UnsupportedCapability):
        backend.validate_caller_write_approvals(True, True)
    backend.validate_caller_write_approvals(False, True)
    from harflow_mcp.workflows import WorkflowError, _candidate
    with pytest.raises(WorkflowError, match="qualified uncapped backend"):
        _candidate({"backend": "codex", "caller_approvals": True})


# --- dispatch-time refusals ------------------------------------------------------------------------------------

@pytest.fixture
def prepared(tmp_path_factory, monkeypatch):
    base = tmp_path_factory.mktemp("prepare")
    repo, home, tmpdir = base / "repo", base / "codex-home", base / "tmp"
    for path in (repo, home, tmpdir):
        path.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setenv("CODEX_HOME", str(home))
    monkeypatch.setenv("TMPDIR", str(tmpdir))
    monkeypatch.setattr(backends, "version", lambda b: "codex-cli 0.163.0")
    monkeypatch.setattr(sys, "platform", "darwin")
    return SimpleNamespace(repo=repo, home=home, base=base)


def exec_invocation(repo, freedom="write_in_repo", prompt="hello", **kwargs):
    return get("codex").build_start_argv(prompt, repo=repo, freedom=freedom, session_id=None, model=kwargs.get("model"), max_turns=None,
                                         reasoning_effort=kwargs.get("effort"), network=kwargs.get("network"))


def configure(repo, freedom="write_in_repo", **kwargs):
    backend = get("codex")
    invocation = exec_invocation(repo, freedom, **kwargs)
    return backend.configure_caller_approvals(invocation, freedom, None, backend.qualify_caller_approvals(), False)


def test_configure_builds_the_exact_app_server_invocation(prepared):
    invocation = configure(prepared.repo, model="gpt-x", effort="high", network=True)
    ctx = invocation.control_context
    assert ctx["cwd"] == os.path.realpath(prepared.repo) and ctx["git_root"] == ctx["cwd"] and ctx["network"] is True and ctx["model"] == "gpt-x" and ctx["effort"] == "high"
    assert ctx["prompt"] == "hello" and ctx["thread_id"] is None and ctx["version"] == "0.163.0"
    assert invocation.argv == app.argv(ctx) and invocation.control_pipe and not invocation.live_input and invocation.stdin_mode == "devnull" and invocation.initial_input is None
    assert invocation.caller_approvals and invocation.caller_approval_version == "0.163.0" and invocation.identity_markers == ("codex", "app-server")
    get("codex").assert_safe(invocation, "write_in_repo")
    get("codex").assert_safe(invocation, "write_in_repo", True)
    with pytest.raises(UnsafeInvocationError):
        get("codex").assert_safe(invocation, "write_in_repo", False)


def test_publish_defaults_to_network_and_a_large_prompt_travels_in_the_context(prepared):
    assert configure(prepared.repo, "publish").control_context["network"] is True
    big = "x" * (40 * 1024)
    assert configure(prepared.repo, prompt=big).control_context["prompt"] == big


def test_resume_carries_the_thread_id(prepared):
    backend = get("codex")
    invocation = backend.build_resume_argv("again", repo=prepared.repo, freedom="write_in_repo", session_id="thread-9", model=None, max_turns=None, reasoning_effort=None)
    configured = backend.configure_caller_approvals(invocation, "write_in_repo", None, "0.163.0", False)
    assert configured.control_context["thread_id"] == "thread-9" and configured.control_context["prompt"] == "again"


def test_without_caller_approvals_the_exec_invocation_is_untouched(prepared):
    invocation = exec_invocation(prepared.repo)
    assert invocation.argv[:2] == ["codex", "exec"] and not invocation.control_pipe and invocation.control_context is None and invocation.identity_markers is None


@pytest.mark.parametrize("freedom", ["read_only", "unrestricted"])
def test_other_freedoms_are_refused(prepared, freedom):
    with pytest.raises(UnsupportedCapability, match="write_in_repo or publish"):
        get("codex").validate_caller_approvals(True, freedom, None)
    with pytest.raises(UnsupportedCapability):
        get("codex").configure_caller_approvals(exec_invocation(prepared.repo, freedom), freedom, None, "0.163.0", False)


def test_turn_caps_native_children_and_write_approvals_are_refused(prepared):
    backend = get("codex")
    with pytest.raises(UnsupportedCapability):
        backend.validate_caller_approvals(True, "write_in_repo", 3)
    with pytest.raises(UnsupportedCapability):
        backend.validate_caller_approvals(True, "write_in_repo", None, True)
    with pytest.raises(UnsupportedCapability, match="caller_write_approvals"):
        backend.configure_caller_approvals(exec_invocation(prepared.repo), "write_in_repo", None, "0.163.0", True)


def test_a_directory_without_a_git_root_is_refused(prepared, tmp_path_factory):
    plain = tmp_path_factory.mktemp("plain")
    with pytest.raises(UnsupportedCapability, match="Git"):
        configure(plain)


def test_a_non_darwin_platform_refuses_at_qualification_and_at_configuration(prepared, monkeypatch):
    monkeypatch.setattr(sys, "platform", "linux")
    with pytest.raises(UnsupportedCapability, match="measured only on darwin"):
        get("codex").qualify_caller_approvals()
    with pytest.raises(UnsupportedCapability):
        get("codex").configure_caller_approvals(exec_invocation(prepared.repo), "write_in_repo", None, "0.163.0", False)


BYPASSES = [
    'prefix_rule(pattern=["touch"])',                                  # the default decision is allow (measured)
    'prefix_rule(pattern=["touch"], decision="al" + "low")',
    'prefix_rule(pattern=["touch"], decision="\\x61llow")',
    'prefix_rule(pattern=["touch"], decision="allow")',
    "prefix_rule(pattern=['x'], decision='allow')",
    'd = "allow"\nprefix_rule(pattern=["x"], decision=d)',
    'prefix_rule(pattern=["touch"], decision="prompt" if True else "allow")',
    'prefix_rule(pattern=["touch"], decision="pro" "mpt")',
    'prefix_rule(pattern=["touch"], decision=("allow"))',
    'prefix_rule(pattern=["touch"], decision="PROMPT")',
    'prefix_rule(pattern=["touch" + "x"], decision="prompt")',
    'prefix_rule(pattern=["touch"], decision="prompt", justification="a" + "b")',
    'prefix_rule(pattern=["touch"], decision="prompt", extra="x")',
    'prefix_rule(["touch"], decision="prompt")',
    'prefix_rule(pattern=[], decision="prompt")',
    'prefix_rule(pattern=["a"], decision="prompt", decision="forbidden")',
    'print("hi")',
    'load("x", "y")',
    'def f(): pass',
    'prefix_rule(pattern=["a"], decision="prompt"); x = 1',
    'prefix_rule(pattern=["a"], decision="""prompt""")',
    'prefix_rule(pattern=["a"], decision="prompt"',
    "\x00",
]
CONFORMING = [
    "", "# only a comment\n\n",
    'prefix_rule(pattern=["rm"], decision="forbidden")',
    "prefix_rule(pattern=['ls', 'x'], decision='prompt', justification='asks')",
    'prefix_rule(pattern=[["git", "hg"], "push"], decision="prompt", match=[["git", "push"]], not_match=["git status"])',
    '# c\nprefix_rule(\n    pattern=["a"],\n    decision="forbidden",\n)  # trailing\n',
]


@pytest.mark.parametrize("rule", BYPASSES)
def test_every_rules_form_outside_the_restricted_grammar_is_refused(rule):
    assert app.rules_problem(rule + "\n") is not None


@pytest.mark.parametrize("rule", CONFORMING)
def test_conforming_prompt_and_forbidden_rules_are_accepted(rule):
    assert app.rules_problem(rule) is None


@pytest.mark.parametrize("rule", BYPASSES)
def test_a_non_conforming_rules_file_in_codex_home_is_refused_on_every_dispatch(prepared, rule):
    (prepared.home / "rules").mkdir()
    (prepared.home / "rules" / "default.rules").write_text(rule + "\n")
    with pytest.raises(UnsupportedCapability, match="rules file"):
        configure(prepared.repo)
    (prepared.home / "rules" / "default.rules").write_text('prefix_rule(pattern=["rm"], decision="forbidden")\nprefix_rule(pattern=["ls"], decision="prompt")\n')
    configure(prepared.repo)


def test_only_the_files_codex_loads_are_scanned_and_symlinks_refuse(prepared):
    rules = prepared.home / "rules"
    rules.mkdir()
    bad = 'prefix_rule(pattern=["touch"], decision="allow")\n'
    for name in (".DS_Store", "notes.txt", "readme", "x.rules.bak"):
        (rules / name).write_text(bad)
    (rules / "sub").mkdir()
    (rules / "sub" / "deep.rules").write_text(bad)   # subdirectories are not loaded (measured)
    configure(prepared.repo)
    (rules / ".hidden.rules").write_text(bad)         # hidden *.rules files are loaded (measured)
    with pytest.raises(UnsupportedCapability, match="rules file"):
        configure(prepared.repo)
    (rules / ".hidden.rules").unlink()
    (prepared.base / "elsewhere.rules").write_text('prefix_rule(pattern=["a"], decision="prompt")\n')
    (rules / "link.rules").symlink_to(prepared.base / "elsewhere.rules")
    with pytest.raises(UnsupportedCapability, match="symlink"):
        configure(prepared.repo)
    (rules / "link.rules").unlink()
    (rules / "linkdir").symlink_to(prepared.base)
    with pytest.raises(UnsupportedCapability, match="symlink"):
        configure(prepared.repo)


def test_a_symlinked_rules_directory_is_refused(prepared):
    (prepared.base / "real-rules").mkdir()
    (prepared.home / "rules").symlink_to(prepared.base / "real-rules")
    with pytest.raises(UnsupportedCapability, match="symlinked"):
        configure(prepared.repo)


@pytest.mark.skipif(not os.environ.get("HARFLOW_CLI_INTEGRATION"), reason="opt in with HARFLOW_CLI_INTEGRATION=1")
@pytest.mark.cli_integration
def test_the_rules_grammar_agrees_with_codex_execpolicy_check(tmp_path):
    """No file the grammar accepts evaluates to allow; the bypass forms that do evaluate to allow are all refused."""
    import shutil
    codex = shutil.which("codex")
    if codex is None or subprocess.run([codex, "--version"], capture_output=True, text=True).stdout.strip() != "codex-cli 0.163.0":
        pytest.skip("measured on codex-cli 0.163.0")
    env = {**{k: v for k, v in os.environ.items() if not k.startswith(("CODEX", "OPENAI"))}, "HOME": str(tmp_path), "CODEX_HOME": str(tmp_path / "ch")}
    (tmp_path / "ch").mkdir()

    def decision(text):
        path = tmp_path / "probe.rules"
        path.write_text(text)
        out = subprocess.run([codex, "execpolicy", "check", "--rules", str(path), "touch", "x"], capture_output=True, text=True, env=env, timeout=30)
        if out.returncode != 0:
            return "error"
        return json.loads(out.stdout).get("decision", "none")

    for rule in CONFORMING:
        assert app.rules_problem(rule) is None
        assert decision(rule) != "allow", rule
    allows = [rule for rule in BYPASSES if decision(rule + "\n") == "allow"]
    assert len(allows) >= 5, "the probe should have found the measured allow bypasses"
    for rule in allows:
        assert app.rules_problem(rule + "\n") is not None, rule


def test_an_unreadable_rules_file_is_refused(prepared):
    (prepared.home / "rules").mkdir()
    (prepared.home / "rules" / "binary.rules").write_bytes(b"\xff\xfe\x00")
    with pytest.raises(UnsupportedCapability, match="unparseable"):
        configure(prepared.repo)
    (prepared.home / "rules" / "binary.rules").write_text("not python at all (((\n")
    with pytest.raises(UnsupportedCapability, match="rules file"):
        configure(prepared.repo)


@pytest.mark.parametrize("where", ["workspace", "tmpdir", "tmp"])
def test_a_codex_home_under_a_sandbox_writable_root_is_refused(prepared, monkeypatch, where):
    home = {"workspace": prepared.repo / ".codex-home", "tmpdir": prepared.base / "tmp" / "home", "tmp": Path("/tmp") / f"codex-home-{os.getpid()}"}[where]
    home.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("CODEX_HOME", str(home))
    try:
        with pytest.raises(UnsupportedCapability, match="sandbox-writable"):
            configure(prepared.repo)
    finally:
        if where == "tmp":
            home.rmdir()


def test_a_profile_or_a_broken_config_in_codex_home_is_refused(prepared):
    (prepared.home / "config.toml").write_text('profile = "work"\n')
    with pytest.raises(UnsupportedCapability, match="profile"):
        configure(prepared.repo)
    (prepared.home / "config.toml").write_text("not = [valid\n")
    with pytest.raises(UnsupportedCapability, match="invalid"):
        configure(prepared.repo)
    (prepared.home / "config.toml").write_text('[mcp_servers.mine]\ncommand = "x"\n[shell_environment_policy.set]\nFOO = "1"\n')
    invocation = configure(prepared.repo)
    caveats = get("codex").approval_enforcement(get("codex").enforcement("write_in_repo"), "0.163.0", True, False).caveats
    assert any("mine" in c for c in caveats) and any("FOO" in c for c in caveats)
    assert invocation.control_pipe


def test_enforcement_and_preview_text_state_the_real_limits(prepared):
    backend = get("codex")
    enforcement = backend.approval_enforcement(backend.enforcement("write_in_repo"), "0.163.0", True, False)
    text = " ".join(enforcement.caveats)
    assert enforcement.caller_approval_tools == ("commandExecution",) and "app-server" in enforcement.mechanism
    for phrase in ("outside the OS sandbox", "Git config and hooks", "CODEX_HOME", "Harflow's own state", "not path-checked", "MCP tool calls that need approval are always rejected", "own .codex config, rules and hooks do not apply"):
        assert phrase in text or phrase.replace("Git config", "Git config") in text, phrase
    assert backend.approval_enforcement(backend.enforcement("write_in_repo"), None, True, False) == backend.enforcement("write_in_repo")
    assert backend.approval_transport() == "codex_app_server" and "macOS only" in backend.approval_limitations(False)
    assert backend.protected_config_paths(prepared.repo) == [Path(os.path.realpath(prepared.home))]
    assert backend.approval_paths({"action": {"tool_name": "commandExecution", "input": {}}}) == [] and backend.approval_write_tools() == frozenset()


def test_assert_safe_accepts_only_the_exact_app_server_shape(prepared):
    from dataclasses import replace
    backend = get("codex")
    good = configure(prepared.repo)
    backend.assert_safe(good, "write_in_repo")
    tampered = {
        "extra flag": replace(good, argv=[*good.argv[:-2], "--enable", "x", *good.argv[-2:]]),
        "weaker pin": replace(good, argv=[a.replace('"on-request"', '"never"') for a in good.argv]),
        "websocket": replace(good, argv=[*good.argv[:-1], "ws://127.0.0.1:1"]),
        "no pipe": replace(good, control_pipe=False, control_context=None),
        "wrong markers": replace(good, identity_markers=("codex",)),
        "write approvals": replace(good, caller_write_approvals=True),
        "extra paths": replace(good, extra_writable_paths=("/x",)),
        "commands": replace(good, allowed_commands=("ls",)),
        "version drift": replace(good, caller_approval_version="0.162.0"),
        "context drift": replace(good, control_context={**good.control_context, "freedom": "publish"}),
        "context junk": replace(good, control_context={**good.control_context, "extra": 1}),
        "env": replace(good, env={"A": "b"}),
    }
    for name, invocation in tampered.items():
        with pytest.raises((UnsafeInvocationError, UnsupportedCapability, backends.base.InvocationEnvironmentError)):
            backend.assert_safe(invocation, "write_in_repo")
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(good, "publish")
    with pytest.raises(UnsafeInvocationError, match="never an exec argv"):
        backend.assert_safe(replace(exec_invocation(prepared.repo), caller_approvals=True), "write_in_repo")
    assert backend.assert_safe(exec_invocation(prepared.repo), "write_in_repo") is None


# --- correlated terminal and session evidence ----------------------------------------------------------------------

def live(cwd, **over):
    ctx = make_ctx(cwd)
    state = started(ctx, **over)
    acc = Accumulator()
    acc.stream_state.update(control_context=ctx, app_server=state)
    return state, acc


def feed(backend, state, acc, frame):
    try:
        app.step(state, frame)
    except ApprovalRefused:
        pass
    backend.ingest(frame, acc)


def turn_done(thread, turn, status="completed", **extra):
    return {"method": "turn/completed", "params": {"threadId": thread, "turn": {"id": turn, "status": status, **extra}}}


def message(text="ok", thread=THREAD, turn=TURN, method="item/completed"):
    return {"method": method, "params": {"threadId": thread, "turnId": turn, "item": {"type": "agentMessage", "id": "m", "text": text}}}


MESSAGE = message()


@pytest.mark.parametrize("frame", [turn_done(THREAD, "historic-turn"), turn_done("other-thread", TURN)])
def test_a_historical_or_foreign_terminal_never_makes_a_run_succeed(cwd, frame):
    backend = get("codex")
    state, acc = live(cwd)
    feed(backend, state, acc, MESSAGE)
    feed(backend, state, acc, frame)
    assert state.terminal is None and "app_server_turn_status" not in acc.stream_state and acc.terminal is None
    assert backend.classify(acc, 0) == "failed" and backend.classify(acc, 1) == "failed" and backend.classify(acc, None) == "failed"


def test_only_the_active_turns_completed_terminal_succeeds_and_a_refusal_after_it_fails_the_run(cwd):
    backend = get("codex")
    state, acc = live(cwd)
    feed(backend, state, acc, MESSAGE)
    feed(backend, state, acc, turn_done(THREAD, TURN))
    assert backend.classify(acc, 0) == "completed" and backend.classify(acc, 3) == "failed"
    state.failed = "a later refusal"
    assert backend.classify(acc, 0) == "failed"


@pytest.mark.parametrize("status", ["failed", "interrupted"])
def test_a_failed_or_interrupted_active_terminal_fails(cwd, status):
    backend = get("codex")
    state, acc = live(cwd)
    feed(backend, state, acc, MESSAGE)
    feed(backend, state, acc, turn_done(THREAD, TURN, status, error={"message": "x"}))
    assert backend.classify(acc, 0) == "failed"


def test_a_terminal_before_verification_or_a_refused_frame_counts_for_nothing(cwd):
    backend = get("codex")
    ctx = make_ctx(cwd)
    state = app.State(ctx)
    acc = Accumulator()
    acc.stream_state.update(control_context=ctx, app_server=state)
    feed(backend, state, acc, MESSAGE)
    feed(backend, state, acc, turn_done(THREAD, TURN))
    assert state.terminal is None and state.failed and backend.classify(acc, 0) == "failed"


def test_an_unmatched_or_duplicate_thread_response_cannot_change_the_session(cwd):
    backend = get("codex")
    state, acc = live(cwd)
    backend.ingest({"id": 2, "result": thread_result(state.ctx)}, acc)
    assert acc.session_id == THREAD
    backend.ingest({"id": 99, "result": thread_result(state.ctx, thread={"id": "intruder", "cliVersion": VERSION})}, acc)
    assert acc.session_id == THREAD


def test_a_request_after_the_terminal_turn_stops_the_run(cwd):
    state = started(make_ctx(cwd))
    app.step(state, turn_done(THREAD, TURN))
    assert not state.admissible
    with pytest.raises(ApprovalRefused, match="after the turn ended"):
        app.decode(command(), qual(state))


def test_after_a_refusal_no_completion_is_attributed_to_an_approval(cwd):
    state = started(make_ctx(cwd))
    app.decode(command(item="call-1"), qual(state))
    done = {"method": "item/completed", "params": {"threadId": THREAD, "turnId": TURN, "item": {"type": "commandExecution", "id": "call-1", "status": "completed", "exitCode": 0}}}
    assert app.outcomes(done, state) == [("call-1", "succeeded")]
    with pytest.raises(ApprovalRefused):
        app.step(state, {"method": "hook/started", "params": {}})
    assert app.outcomes(done, state) == []


def test_a_tripwire_carries_a_bounded_audit_reason(cwd):
    state = started(make_ctx(cwd))
    with pytest.raises(ApprovalRefused) as raised:
        app.step(state, {"method": "guardianWarning", "params": {}})
    assert raised.value.audit == "tripwire guardianWarning"
    with pytest.raises(ApprovalRefused) as other:
        answer(app.State(make_ctx(cwd)), "initialize") if False else app.step(app.State(make_ctx(cwd)), {"method": "item/started", "params": {}})
    assert other.value.audit is None


@pytest.mark.parametrize("foreign", [message("from another thread", thread="other-thread"), message("from a historical turn", turn="old-turn"),
                                     {"method": "item/completed", "params": {"item": {"type": "agentMessage", "id": "m", "text": "uncorrelated"}}}])
def test_foreign_or_historical_items_feed_neither_the_summary_nor_success(cwd, foreign):
    backend = get("codex")
    state, acc = live(cwd)
    feed(backend, state, acc, foreign)
    feed(backend, state, acc, turn_done(THREAD, TURN))
    assert acc.summary is None and not acc.saw_final_message
    assert backend.classify(acc, 0) == "failed", "a valid terminal without a final message of the active turn is no success"
    assert backend.normalize(foreign, acc) == []
    feed(backend, state, acc, message("the real answer"))
    assert acc.summary == "the real answer" and backend.classify(acc, 0) == "completed"
    feed(backend, state, acc, foreign)
    assert acc.summary == "the real answer"


def test_item_started_frames_for_other_turns_are_not_normalized(cwd):
    backend = get("codex")
    state, acc = live(cwd)
    started_frame = lambda turn: {"method": "item/started", "params": {"threadId": THREAD, "turnId": turn, "item": {"type": "commandExecution", "id": "c", "command": "ls", "status": "inProgress"}}}
    assert backend.normalize(started_frame("old-turn"), acc) == []
    assert [e["kind"] for e in backend.normalize(started_frame(TURN), acc)] == ["tool_call"]
