"""Codex native invocation isolation and lifecycle authority."""
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest

from polybridge.backends.codex import CodexBackend, UnsafeInvocationError
from polybridge.backends.codex_native import CodexNativeAdapter, NATIVE_ARGS, child_configuration

PARENT = "00000000-0000-0000-0000-000000000001"
CHILD = "00000000-0000-0000-0000-000000000002"


def invocation(tmp_path, resume=False):
    backend = CodexBackend()
    kwargs = dict(repo=tmp_path, freedom="read_only", model="gpt-6.1-sol", max_turns=None, reasoning_effort=None)
    return backend.build_resume_argv("assignment", session_id=PARENT, **kwargs) if resume else backend.build_start_argv("assignment", session_id=None, **kwargs)


@pytest.mark.parametrize("resume", [False, True])
def test_native_suffix_is_isolated_from_ordinary_options(tmp_path, resume):
    adapter, backend = CodexNativeAdapter(), CodexBackend()
    native = adapter.configure(invocation(tmp_path, resume))
    backend.assert_safe(native, "read_only")
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(native, native_subagent=False), "read_only")
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(native, "write_in_repo")
    argv = list(native.argv)
    index = argv.index("--") - (1 if resume else 0)
    argv[index:index] = NATIVE_ARGS
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(native, argv=argv), "read_only")


def test_native_options_cannot_accept_ambient_model_or_effort(tmp_path):
    adapter, backend = CodexNativeAdapter(), CodexBackend()
    native = adapter.configure(invocation(tmp_path))
    argv = list(native.argv)
    argv[argv.index("-m") + 1] = "foreign-model"
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(native, argv=argv), "read_only")
    argv = list(native.argv)
    index = argv.index("--")
    argv[index:index] = ["-c", 'model_reasoning_effort="low"']
    with pytest.raises(UnsafeInvocationError):
        backend.assert_safe(replace(native, argv=argv), "read_only")


def write_rollout(tmp_path, monkeypatch, **context_overrides):
    monkeypatch.setenv("CODEX_HOME", str(tmp_path))
    directory = tmp_path / "sessions" / "2026" / "10" / "06"
    directory.mkdir(parents=True, exist_ok=True)
    context = {"model": "gpt-6.1-sol", "approval_policy": "never", "sandbox_policy": {"type": "read-only"}, "cwd": str(tmp_path), **context_overrides}
    meta = {"id": CHILD, "cli_version": "0.162.0", "model_provider": "local", "cwd": str(tmp_path), "source": {"subagent": {"thread_spawn": {"parent_thread_id": PARENT, "depth": 1}}}}
    file = directory / f"rollout-2026-10-06T00-00-00-{CHILD}.jsonl"
    file.write_text("\n".join(json.dumps({"type": kind, "payload": payload}) for kind, payload in [("session_meta", meta), ("turn_context", context)]) + "\n")
    return file


def test_correlated_configuration_is_observed(tmp_path, monkeypatch):
    write_rollout(tmp_path, monkeypatch)
    metadata = child_configuration(CHILD, {"owner_session_id": PARENT, "expected_repo": str(tmp_path)})
    assert metadata["model"] == "gpt-6.1-sol"
    assert metadata["settings_source"] == "codex_child_rollout"
    with pytest.raises(ValueError, match="provenance"):
        child_configuration(CHILD, {"owner_session_id": CHILD, "expected_repo": str(tmp_path)})


@pytest.mark.parametrize("context", [{"model": "foreign-model"}, {"approval_policy": "on-request"}, {"sandbox_policy": {"type": "workspace-write"}}, {"sandbox_policy": {"type": "read-only", "network_access": True}}, {"cwd": "/foreign"}])
def test_configuration_rejects_model_access_and_repo_drift(tmp_path, monkeypatch, context):
    write_rollout(tmp_path, monkeypatch, **context)
    with pytest.raises(ValueError):
        child_configuration(CHILD, {"owner_session_id": PARENT, "expected_repo": str(tmp_path)})


def test_parent_completion_without_cleanup_fails_closed():
    with pytest.raises(ValueError, match="acknowledgement"):
        CodexNativeAdapter().finalize("nonce", {})


def test_foreign_parent_is_rejected():
    with pytest.raises(ValueError, match="foreign"):
        CodexNativeAdapter().observe({"type": "thread.started", "thread_id": CHILD}, "nonce", {"owner_session_id": PARENT})


def test_eligibility_uses_real_cli_version_shape(monkeypatch):
    from polybridge import backends
    monkeypatch.setattr(backends, "version", lambda _: "codex-cli 0.162.0")
    parent = SimpleNamespace(backend="codex", freedom="read_only", network=None, model="gpt-6.1-sol", reasoning_effort=None, max_turns=None)
    candidate = {"model": parent.model}
    settings = {"freedom": "read_only", "network": None}
    adapter = CodexNativeAdapter()
    assert adapter.eligible(parent, candidate, settings) is None
    assert "effort" in adapter.eligible(parent, {**candidate, "reasoning_effort": "low"}, settings)
    assert "turn caps" in adapter.eligible(parent, {**candidate, "max_turns": 100}, settings)
    monkeypatch.setattr(backends, "version", lambda _: "codex-cli 0.160.2")
    assert "0.162.0" in adapter.eligible(parent, candidate, settings)


def native_rollouts(tmp_path, monkeypatch, nonce="nonce", assignment="assignment", mutation=None, effort="medium"):
    adapter = CodexNativeAdapter()
    args = adapter.spawn_arguments(assignment, nonce)
    path = "/root/" + args["task_name"]
    turn = "parent-turn"
    def record(kind, **payload):
        return {"type": kind, "payload": payload}
    child_file = write_rollout(tmp_path, monkeypatch)
    child_records = [json.loads(line) for line in child_file.read_text().splitlines()]
    child_records[0]["payload"]["source"]["subagent"]["thread_spawn"]["agent_path"] = path
    child_records[1]["payload"].update(effort=effort)
    child_records.append(record("event_msg", type="task_complete", last_agent_message='{"status":"succeeded"}'))
    if mutation == "missing_child_complete":
        child_records.pop()
    if mutation == "effort":
        child_records[1]["payload"]["effort"] = "high"
    if mutation == "provider":
        child_records[0]["payload"]["model_provider"] = "foreign"
    child_file.write_text("\n".join(json.dumps(r) for r in child_records) + "\n")
    records = [
        record("session_meta", id=PARENT, cli_version="0.162.0", model_provider="local"),
        record("turn_context", turn_id=turn, effort=effort, model="gpt-6.1-sol", cwd=str(tmp_path), approval_policy="never", sandbox_policy={"type": "read-only"}),
        record("response_item", type="function_call", namespace="collaboration", name="spawn_agent", call_id="spawn", arguments=json.dumps(args), internal_chat_message_metadata_passthrough={"turn_id": turn}),
        record("event_msg", type="item_completed", thread_id=PARENT, turn_id=turn, item={"type": "SubAgentActivity", "id": "spawn", "kind": "started", "agent_thread_id": CHILD, "agent_path": path}),
        record("response_item", type="function_call_output", call_id="spawn", output=json.dumps({"task_name": path})),
        record("response_item", type="function_call", namespace="collaboration", name="wait_agent", call_id="wait", arguments="{}", internal_chat_message_metadata_passthrough={"turn_id": turn}),
        record("event_msg", type="item_completed", thread_id=PARENT, turn_id=turn, item={"type": "SubAgentActivity", "id": "settled", "kind": "completed", "agent_thread_id": CHILD, "agent_path": path}),
        record("event_msg", type="task_complete", turn_id=turn, last_agent_message=json.dumps({"native_dispatch_nonce": nonce, "settled": True})),
    ]
    if mutation == "nonce":
        records[2]["payload"]["arguments"] = json.dumps({**args, "message": "foreign"})
    elif mutation == "foreign":
        records[3]["payload"]["thread_id"] = CHILD
    elif mutation == "extra_spawn":
        records.append(records[2])
    elif mutation == "missing_terminal":
        del records[-2]
    elif mutation == "early_ack":
        records[-1], records[-2] = records[-2], records[-1]
    elif mutation == "wrong_output":
        records[4]["payload"]["output"] = "{}"
    elif mutation == "parent_access":
        records[1]["payload"]["sandbox_policy"] = {"type": "danger-full-access"}
    elif mutation == "unexpected_collaboration":
        records[5]["payload"]["name"] = "followup_task"
    elif mutation in {"parent_shell", "parent_patch"}:
        records.insert(3, record("response_item", type="function_call" if mutation == "parent_shell" else "custom_tool_call", namespace="functions", name="exec_command" if mutation == "parent_shell" else "apply_patch", call_id="untracked", arguments="{}", internal_chat_message_metadata_passthrough={"turn_id": turn}))
    parent_file = child_file.with_name(f"rollout-parent-{PARENT}.jsonl")
    parent_file.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def lifecycle(nonce="nonce", assignment="assignment", session=PARENT, child=CHILD):
    return [
        {"type": "thread.started", "thread_id": session},
        {"type": "item.completed", "item": {"id": "ack", "type": "agent_message", "text": json.dumps({"native_dispatch_nonce": nonce, "settled": True})}},
        {"type": "turn.completed", "usage": {}},
    ]


def test_stream_requires_correlated_rollout_terminal_and_parent_ack(tmp_path, monkeypatch):
    native_rollouts(tmp_path, monkeypatch)
    state = {"owner_session_id": PARENT, "assignment": "assignment", "expected_repo": str(tmp_path)}
    adapter = CodexNativeAdapter()
    for event in lifecycle():
        assert adapter.observe(event, "nonce", state) == []
    updates = adapter.finalize("nonce", state)
    assert [update["native_update"] for update in updates] == ["started", "settled"]
    assert updates[-1]["observed_metadata"]["approval_policy"] == "never"
    assert updates[-1]["observed_metadata"]["reasoning_effort"] == "medium"
    assert adapter.finalize("nonce", state) == []


@pytest.mark.parametrize("mutation", ["nonce", "foreign", "extra_spawn", "missing_terminal", "early_ack", "wrong_output", "missing_child_complete", "effort", "provider", "parent_access", "unexpected_collaboration", "parent_shell", "parent_patch"])
def test_native_lifecycle_rejects_uncorrelated_or_incomplete_evidence(tmp_path, monkeypatch, mutation):
    native_rollouts(tmp_path, monkeypatch, mutation=mutation)
    state = {"owner_session_id": PARENT, "assignment": "assignment", "expected_repo": str(tmp_path)}
    adapter = CodexNativeAdapter()
    for event in lifecycle():
        adapter.observe(event, "nonce", state)
    with pytest.raises(ValueError):
        adapter.finalize("nonce", state)


def test_partial_rollout_cannot_be_accepted_as_terminal(tmp_path, monkeypatch):
    from polybridge.backends.codex_native import rollout_records
    native_rollouts(tmp_path, monkeypatch)
    file = next((tmp_path / "sessions").glob(f"*/*/*/rollout-parent-{PARENT}.jsonl"))
    file.write_bytes(file.read_bytes().rstrip(b"\n"))
    with pytest.raises(ValueError, match="incomplete"):
        rollout_records(PARENT)


def failed_rollouts(tmp_path, monkeypatch, mutation=None):
    native_rollouts(tmp_path, monkeypatch)
    directory = tmp_path / "sessions" / "2026" / "10" / "06"
    child_file = next(directory.glob(f"*-{CHILD}.jsonl"))
    child_records = [json.loads(line) for line in child_file.read_text().splitlines()]
    child_records[-1]["payload"].update(last_agent_message=None, error={"message": "fixture native child API failure", "codex_error_info": "invalid_prompt"})
    if mutation == "conflicting_report":
        child_records[-1]["payload"]["last_agent_message"] = "succeeded"
    child_file.write_text("\n".join(json.dumps(r) for r in child_records) + "\n")
    parent_file = next(directory.glob(f"*-{PARENT}.jsonl"))
    records = [json.loads(line) for line in parent_file.read_text().splitlines()]
    path = "/root/" + CodexNativeAdapter.spawn_arguments("assignment", "nonce")["task_name"]
    records[-2] = {"type": "response_item", "payload": {"type": "agent_message", "author": path, "recipient": "/root", "internal_chat_message_metadata_passthrough": {"turn_id": "parent-turn"}, "content": [{"type": "input_text", "text": f"Message Type: FINAL_ANSWER\nTask name: /root\nSender: {path}\nPayload:\nAgent errored: fixture native child API failure\n\nThis agent's turn failed. If you still need this agent, use the available collaboration tools to give it another task."}]}}
    if mutation == "missing_notification":
        del records[-2]
    elif mutation == "foreign_child":
        records[-2]["payload"]["author"] = "/root/foreign"
    elif mutation == "foreign_turn":
        records[-2]["payload"]["internal_chat_message_metadata_passthrough"]["turn_id"] = "foreign-turn"
    elif mutation == "late_notification":
        records[-1], records[-2] = records[-2], records[-1]
    parent_file.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def test_definitive_child_error_settles_as_failed(tmp_path, monkeypatch):
    failed_rollouts(tmp_path, monkeypatch)
    adapter = CodexNativeAdapter()
    state = {"owner_session_id": PARENT, "assignment": "assignment", "expected_repo": str(tmp_path)}
    for event in lifecycle():
        adapter.observe(event, "nonce", state)
    updates = adapter.finalize("nonce", state)
    assert state["terminal"] is True
    assert updates[-1]["status"] == "failed"
    assert updates[-1]["summary"] == "fixture native child API failure"
    assert updates[-1]["observed_metadata"]["model"] == "gpt-6.1-sol"


@pytest.mark.parametrize("mutation", ["missing_notification", "foreign_child", "foreign_turn", "late_notification", "conflicting_report"])
def test_incomplete_child_failure_evidence_stays_uncertain(tmp_path, monkeypatch, mutation):
    failed_rollouts(tmp_path, monkeypatch, mutation)
    adapter = CodexNativeAdapter()
    state = {"owner_session_id": PARENT, "assignment": "assignment", "expected_repo": str(tmp_path)}
    for event in lifecycle():
        adapter.observe(event, "nonce", state)
    with pytest.raises(ValueError):
        adapter.finalize("nonce", state)
    assert not state.get("terminal")
