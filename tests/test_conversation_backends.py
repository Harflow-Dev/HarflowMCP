"""Independent scripted native peers: no installed binaries or provider calls."""
import json
from dataclasses import replace

import pytest

from harflow_mcp.backends.base import Invocation
from harflow_mcp.backends.claude import ClaudeBackend
from harflow_mcp.backends.codex import CodexBackend
from harflow_mcp.backends.conversation import ConversationRefused
from harflow_mcp.backends.conversation_native import (
    ACPConversationPort, ClaudeConversationPort, CodexConversationPort,
    configure_conversation, conversation_profile, restore_conversation_port,
)


class Peer:
    def __init__(self, port):
        self.port = port
        self.sequence = 0

    def frames(self):
        return [json.loads(frame) for frame in self.port.outgoing_frames()]

    def send(self, frame):
        self.sequence += 1
        return self.port.observe(frame, sequence=self.sequence)

    def answer(self, request, result):
        return self.send({"jsonrpc": "2.0", "id": request["id"], "result": result})


def codex():
    profile = {"backend": "codex", "version": "0.166.0", "cwd": "/repo", "sandbox": "workspace-write",
               "approval_policy": "never", "network": False, "writable_roots": ["/repo"], "caller_approvals": False}
    p = CodexConversationPort(profile)
    peer = Peer(p)
    init, = peer.frames()
    peer.answer(init, {"userAgent": "codex/0.166.0"})
    notify, start = peer.frames()
    assert notify["method"] == "initialized"
    peer.answer(start, {"thread": {"id": "thread-a"}, "approvalPolicy": "never", "approvalsReviewer": "user",
                        "sandbox": {"type": "workspaceWrite", "networkAccess": False, "writableRoots": ["/repo"]}})
    assert p.ready
    return p, peer


def acp(backend="antigravity"):
    profile = {"backend": backend, "version": "1.5.1" if backend == "antigravity" else "2026.10.22-27dcf5f",
               "cwd": "/repo", "mode": "accept-edits" if backend == "antigravity" else "agent", "caller_approvals": False}
    port = ACPConversationPort(profile)
    peer = Peer(port)
    init, = peer.frames()
    peer.answer(init, {"protocolVersion": 1})
    new, = peer.frames()
    peer.answer(new, {"sessionId": "session-a", "modes": {"currentModeId": profile["mode"],
                       "availableModes": [{"id": profile["mode"]}]}})
    assert port.ready
    return port, peer


def claude():
    port = ClaudeConversationPort({"session_id": "session-a", "caller_approvals": False})
    return port, Peer(port)


def start_codex(p, peer, logical="message-1", native="native-1"):
    p.issue("first", logical, receive_fence=peer.sequence)
    request, = peer.frames()
    peer.answer(request, {"turn": {"id": native}})
    return p.identity


def test_codex_interrupt_bytes_available_while_prompt_rpc_outstanding():
    p, peer = codex()
    logical = p.issue("first", "message-1", receive_fence=peer.sequence)
    prompt, = peer.frames()
    # Native started notification precedes the turn/start response.
    peer.send({"method": "turn/started", "params": {"threadId": "thread-a", "turn": {"id": "native-1"}}})
    assert p.identity.logical_turn == logical.logical_turn
    p.interrupt(p.identity)
    cancel, = peer.frames()
    assert cancel["method"] == "turn/interrupt"
    assert cancel["params"] == {"threadId": "thread-a", "turnId": "native-1"}
    peer.answer(cancel, {})
    assert p.interrupt_acknowledged and not p.can_issue
    peer.answer(prompt, {"turn": {"id": "native-1"}})
    observations = peer.send({"method": "turn/completed", "params": {"threadId": "thread-a", "turn": {"id": "native-1", "status": "interrupted"}}})
    assert observations[-1].kind == "response_terminal"
    assert p.can_issue


def test_codex_streaming_text_is_visible_before_response_terminal_and_fenced_afterwards():
    from harflow_mcp.backends.base import Accumulator
    p, peer = codex()
    start_codex(p, peer)
    frame = {'method': 'item/agentMessage/delta', 'params': {
        'threadId': 'thread-a', 'turnId': 'native-1', 'itemId': 'assistant-1', 'delta': 'streamed text'}}
    observed = peer.send(frame)
    normalized = [item for event in observed for item in p.normalize_observation(event, Accumulator())]
    assert normalized == [{'kind': 'assistant_delta', 'text': 'streamed text',
                           'message_id': 'assistant-1', 'block_index': 0}]
    assert p.terminal is None
    p.interrupt(p.identity)
    cancel, = peer.frames()
    peer.answer(cancel, {})
    peer.send({'method': 'turn/completed', 'params': {
        'threadId': 'thread-a', 'turn': {'id': 'native-1', 'status': 'interrupted'}}})
    start_codex(p, peer, logical='message-2', native='native-2')
    late = peer.send(frame)
    assert all(event.kind == 'historical_frame' for event in late)
    assert not [item for event in late for item in p.normalize_observation(event, Accumulator())]


@pytest.mark.parametrize("backend", ["antigravity", "cursor"])
def test_acp_prompt_response_is_terminal_not_cancel_notification(backend):
    p, peer = acp(backend)
    p.issue("one", "one", receive_fence=peer.sequence)
    prompt, = peer.frames()
    p.interrupt(p.identity)
    cancel, = peer.frames()
    assert cancel == {"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": "session-a"}}
    assert not p.can_issue
    peer.answer(prompt, {"stopReason": "cancelled"})
    assert p.can_issue
    p.issue("two", "two", receive_fence=peer.sequence)
    second, = peer.frames()
    assert second["params"]["sessionId"] == prompt["params"]["sessionId"]
    assert second["id"] != prompt["id"]
    # Replayed old RPC cannot finish the new turn.
    peer.answer(prompt, {"stopReason": "end_turn"})
    assert p.terminal is None


def test_acp_selection_echo_required_before_prompt():
    p = ACPConversationPort({"backend": "cursor", "version": "2026.10.22-27dcf5f", "cwd": "/repo", "mode": "plan"})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {"protocolVersion": 1})
    peer.answer(peer.frames()[0], {"sessionId": "s", "modes": {"currentModeId": "agent", "availableModes": [{"id": "plan"}, {"id": "agent"}]}})
    selection, = peer.frames()
    assert selection["method"] == "session/set_mode"
    peer.answer(selection, {})
    assert not p.ready
    with pytest.raises(ConversationRefused):
        p.issue("must not send", "one", receive_fence=peer.sequence)
    peer.send({"method": "session/update", "params": {"sessionId": "s", "update": {"sessionUpdate": "current_mode_update", "currentModeId": "plan"}}})
    assert p.ready


def test_vibe_high_profile_config_options_echoes():
    p = ACPConversationPort({"backend": "vibe", "version": "3.1.0", "cwd": "/repo", "mode": "ask",
                             "model": "mistral-medium-3.5", "effort": "high"})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {"protocolVersion": 1, "agentInfo": {"version": "3.1.0"}})
    options = [{"id": key, "currentValue": value, "options": [{"value": value}]} for key, value in
               [("mode", "ask"), ("model", "mistral-medium-3.5"), ("thinking", "high")]]
    peer.answer(peer.frames()[0], {"sessionId": "s", "configOptions": options, "_meta": {"workspace_trust": {"status": "session"}}})
    for key in ("mode",):
        request, = peer.frames()
        assert request["method"] == "session/set_config_option"
        assert request["params"]["configId"] == key
        peer.answer(request, {"configOptions": options})
    assert p.can_issue
    assert peer.frames() == []


def test_vibe_response_cap_is_applied_to_session_before_any_prompt():
    p = ACPConversationPort({'backend': 'vibe', 'version': '3.1.0', 'cwd': '/repo',
        'mode': 'ask', 'model': 'mistral-medium-3.5', 'effort': 'high', 'max_turns': 7})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {'protocolVersion': 1, 'agentInfo': {'version': '3.1.0'}})
    options = [{'id': key, 'currentValue': value, 'options': [{'value': value}]} for key, value in
        [('mode', 'ask'), ('model', 'mistral-medium-3.5'), ('thinking', 'high')]]
    peer.answer(peer.frames()[0], {'sessionId': 's', 'configOptions': options, '_meta': {'workspace_trust': {'status': 'session'}}})
    mode, = peer.frames()
    peer.answer(mode, {'configOptions': options})
    cap, = peer.frames()
    assert cap['method'] == 'session/set_config_option'
    assert cap['params'] == {'sessionId': 's', 'configId': 'max_turns', 'value': '7'}
    assert not p.can_issue
    peer.answer(cap, {'configOptions': options})
    assert p.can_issue
    assert peer.frames() == []


@pytest.mark.parametrize('prompt', ['--literal caller prose', 'large ' * 7000])
def test_capped_claude_conversion_keeps_cap_policy_and_exact_input(tmp_path, prompt):
    backend = ClaudeBackend()
    original = backend.build_start_argv(prompt, repo=tmp_path, freedom='read_only',
        session_id='12345678-1234-1234-1234-123456789abc', model='opus', max_turns=7, reasoning_effort=None)
    launch = configure_conversation(backend, original, version='2.1.318', freedom='read_only', qualified=True)
    context = launch.invocation.conversation_context
    assert context['initial_prompt'] == prompt
    assert context['max_turns'] == 7
    argv = launch.invocation.argv
    assert argv[argv.index('--max-turns') + 1] == '7'
    assert argv[argv.index('--input-format') + 1] == 'stream-json'
    assert argv[argv.index('--disallowedTools') + 1] == original.argv[original.argv.index('--disallowedTools') + 1]
    assert launch.invocation.initial_input == original.initial_input


def test_claude_requires_fresh_assistant_identity_and_aborted_terminal():
    p, peer = claude()
    p.issue("one", "one", receive_fence=0)
    peer.frames()
    with pytest.raises(ConversationRefused):
        p.interrupt(p.identity)
    peer.send({"type": "stream_event", "session_id": "session-a", "event": {"type": "message_start", "message": {"id": "msg-1"}}})
    p.interrupt(p.identity)
    cancel, = peer.frames()
    peer.send({"type": "control_response", "response": {"request_id": cancel["request_id"], "subtype": "success"}})
    assert p.interrupt_acknowledged and not p.can_issue
    peer.send({"type": "result", "uuid": "result-1", "session_id": "session-a", "terminal_reason": "aborted_streaming"})
    assert p.terminal == "aborted_streaming" and p.can_issue
    p.issue("two", "two", receive_fence=peer.sequence)
    peer.frames()
    peer.send({"type": "stream_event", "session_id": "session-a", "event": {"type": "message_start", "message": {"id": "msg-1"}}})
    assert p.identity.turn_id is None
    peer.send({"type": "result", "session_id": "session-a"})
    assert p.unknown and not p.can_issue


def test_interrupted_tool_never_implies_os_settlement():
    p, peer = acp()
    p.issue("one", "one", receive_fence=peer.sequence)
    prompt, = peer.frames()
    peer.send({"method": "session/update", "params": {"sessionId": "session-a", "update": {"sessionUpdate": "tool_call", "toolCallId": "tool-1", "status": "in_progress"}}})
    p.interrupt(p.identity)
    peer.frames()
    peer.send({"method": "session/update", "params": {"sessionId": "session-a", "update": {"sessionUpdate": "tool_call_update", "toolCallId": "tool-1", "status": "cancelled"}}})
    peer.answer(prompt, {"stopReason": "cancelled"})
    assert not p.can_issue and p.unsettled_effects == {"tool-1"}
    with pytest.raises(ConversationRefused):
        p.release_tool("tool-1", identity=p.identity, trusted_settlement=False)
    p.release_tool("tool-1", identity=p.identity, trusted_settlement=True)
    assert p.can_issue


def test_approval_request_denied_without_opt_in_and_interrupt_expires_pending():
    p, peer = acp()
    p.issue("one", "one", receive_fence=peer.sequence)
    peer.frames()
    peer.send({"jsonrpc": "2.0", "id": 9, "method": "session/request_permission", "params": {"sessionId": "session-a"}})
    denial, = peer.frames()
    assert denial["result"] == {"outcome": {"outcome": "cancelled"}}
    assert not p.pending_approvals
    assert not any(o.kind == "input_consumed" for o in p._observations)


def test_preissued_buffer_fence_wrong_identity_and_duplicate_issue():
    p, peer = acp()
    # Reader has already buffered sequence 3+4, not yet delivered to port.
    p.issue("one", "one", receive_fence=peer.sequence + 2)
    prompt, = peer.frames()
    old = {"method": "session/update", "params": {"sessionId": "session-a", "update": {"sessionUpdate": "tool_call", "toolCallId": "old"}}}
    assert peer.send(old)[0].kind == "historical_frame"
    assert peer.send(old)[0].kind == "historical_frame"
    assert not p.pending_tools
    with pytest.raises(ConversationRefused):
        p.interrupt(replace(p.identity, generation=99))
    peer.answer(prompt, {"stopReason": "end_turn"})
    with pytest.raises(ConversationRefused):
        p.issue("one again", "one", receive_fence=peer.sequence)


def test_codex_wrong_thread_terminal_cannot_settle():
    p, peer = codex()
    start_codex(p, peer)
    peer.send({"method": "turn/completed", "params": {"threadId": "other", "turn": {"id": "native-1", "status": "completed"}}})
    assert p.terminal is None
    p.mark_unknown("partial write")
    assert not p.can_issue


def test_configuration_preserves_claude_exact_policy_and_initial_provenance(tmp_path):
    backend = ClaudeBackend()
    original = backend.build_start_argv("hello", repo=tmp_path, freedom="write_in_repo", session_id="12345678-1234-1234-1234-123456789abc", model=None, max_turns=None, reasoning_effort=None)
    with pytest.raises(ConversationRefused, match="explicit owner qualification"):
        configure_conversation(backend, original, version="2.1.318", freedom="write_in_repo")
    launch = configure_conversation(backend, original, version="2.1.318", freedom="write_in_repo", qualified=True)
    assert launch.invocation.argv == original.argv
    assert launch.invocation.initial_input == original.initial_input
    assert launch.invocation.stdin_mode == original.stdin_mode
    assert launch.invocation.caller_approvals is False
    with pytest.raises(ConversationRefused):
        restore_conversation_port(launch.invocation.conversation_context, validated_profile={})


def test_codex_launch_preserves_never_and_model_network(tmp_path):
    backend = CodexBackend()
    original = backend.build_start_argv("hello", repo=tmp_path, freedom="write_in_repo", session_id=None, model="gpt-6.1-sol", max_turns=None, reasoning_effort="high", network=False)
    profile = conversation_profile(backend, original, version="0.166.0", freedom="write_in_repo", network=False)
    assert profile["approval_policy"] == "never"
    assert profile["model"] == "gpt-6.1-sol" and profile["network"] is False
    assert 'approval_policy="never"' in profile["argv"]
    assert not original.caller_approvals
    with pytest.raises(RuntimeError):
        conversation_profile(backend, replace(original, extra_writable_paths=("/outside",)), version="0.166.0", freedom="write_in_repo")


def test_context_metadata_json_safe_and_independent_of_control_pipe():
    invocation = Invocation(["agent"], stdin_mode="pipe", initial_input=b"initial", conversation_context={"v": 1})
    assert invocation.live_input and not invocation.control_pipe
    with pytest.raises(ValueError, match="JSON-safe"):
        Invocation(["agent"], conversation_context={"bad": object()})
    with pytest.raises(ValueError, match="JSON-safe"):
        Invocation(["agent"], conversation_context={"bad": float("nan")})


def test_vibe_never_sets_model_or_thinking_and_refuses_mismatch():
    p = ACPConversationPort({"backend": "vibe", "version": "3.1.0", "cwd": "/repo", "mode": "ask",
                             "model": "mistral-medium-3.5", "effort": "high"})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {"protocolVersion": 1})
    options = [{"id": key, "currentValue": value, "options": [{"value": "high"}, {"value": value}]} for key, value in
               [("mode", "ask"), ("model", "mistral-medium-3.5"), ("thinking", "max")]]
    peer.answer(peer.frames()[0], {"sessionId": "s", "configOptions": options, "_meta": {"workspace_trust": {"status": "session"}}})
    assert p.unknown and not p.ready
    assert peer.frames() == []


def test_codex_policy_echo_mismatch_refuses_before_any_prompt():
    p = CodexConversationPort({"backend": "codex", "version": "0.166.0", "cwd": "/repo", "sandbox": "read-only",
                               "approval_policy": "never", "network": False, "writable_roots": []})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {"userAgent": "codex/0.166.0"})
    _, start = peer.frames()
    peer.answer(start, {"thread": {"id": "thread-a"}, "approvalPolicy": "on-request", "approvalsReviewer": "user",
                        "sandbox": {"type": "readOnly"}})
    assert p.unknown and not p.ready
    assert peer.frames() == []


def test_codex_usage_cumulative_snapshot_not_sum_and_terminal_not_zero():
    from harflow_mcp.backends.base import Accumulator
    p, peer = codex()
    start_codex(p, peer)
    acc = Accumulator()
    usage = {"inputTokens": 100, "cachedInputTokens": 20, "outputTokens": 8, "reasoningOutputTokens": 2}
    frame = {"method": "thread/tokenUsage/updated", "params": {"threadId": "thread-a", "turnId": "native-1", "tokenUsage": {"total": usage}}}
    observations = peer.send(frame)
    for observation in observations:
        p.normalize_observation(observation, acc)
    assert acc.usage["input_tokens"] == 100
    for observation in peer.send(frame):
        p.normalize_observation(observation, acc)
    assert acc.usage["input_tokens"] == 100
    for observation in peer.send({"method": "turn/completed", "params": {"threadId": "thread-a", "turn": {"id": "native-1", "status": "completed"}}}):
        p.normalize_observation(observation, acc)
    assert acc.usage["input_tokens"] == 100


def test_claude_background_work_blocks_successor_and_interrupted_settlement_is_unknown():
    p, peer = claude()
    p.issue('one', 'one', receive_fence=0)
    peer.frames()
    peer.send({'type': 'stream_event', 'session_id': 'session-a', 'event': {'type': 'message_start', 'message': {'id': 'msg-1'}}})
    peer.send({'type': 'system', 'subtype': 'task_started', 'session_id': 'session-a', 'task_id': 'bg-1', 'is_backgrounded': True})
    p.interrupt(p.identity)
    peer.frames()
    peer.send({'type': 'result', 'uuid': 'result-1', 'session_id': 'session-a', 'terminal_reason': 'aborted_streaming'})
    assert not p.can_issue and 'background:bg-1' in p.pending_tools
    peer.send({'type': 'system', 'subtype': 'task_notification', 'session_id': 'session-a', 'task_id': 'bg-1', 'status': 'completed'})
    assert not p.can_issue and p.unsettled_effects == {'background:bg-1'}


def test_claude_internal_model_steps_share_turn_but_old_ids_stay_historical():
    from harflow_mcp.backends.base import Accumulator
    p, peer = claude()
    p.issue('one', 'one', receive_fence=0)
    peer.frames()
    for mid in ('assistant-1', 'assistant-2'):
        peer.send({'type': 'stream_event', 'session_id': 'session-a', 'event': {
            'type': 'message_start', 'message': {'id': mid}}})
    assert p.identity.turn_id == 'assistant-1'
    body = {'type': 'assistant', 'session_id': 'session-a', 'message': {
        'id': 'assistant-2', 'role': 'assistant', 'content': [
            {'type': 'tool_use', 'id': 'tool-2', 'name': 'Bash', 'input': {'command': 'echo bounded'}}]}}
    observations = peer.send(body)
    assert any(o.kind == 'native_event' for o in observations)
    assert p.pending_tools == {'tool-2'}
    acc = Accumulator()
    normalized = [event for observation in observations for event in p.normalize_observation(observation, acc)]
    assert any(event['kind'] == 'tool_call' and event['call_id'] == 'tool-2' for event in normalized)
    peer.send({'type': 'user', 'session_id': 'session-a', 'message': {'role': 'user', 'content': [
        {'type': 'tool_result', 'tool_use_id': 'tool-2', 'content': 'bounded'}]}})
    peer.send({'type': 'result', 'uuid': 'result-1', 'session_id': 'session-a', 'is_error': False})
    assert p.can_issue
    p.issue('two', 'two', receive_fence=peer.sequence)
    peer.frames()
    assert peer.send(body)[0].kind == 'historical_frame'
    assert not p.pending_tools and p.identity.turn_id is None


def test_vibe_persistent_trust_requirement_refuses_even_with_original_print_flag():
    p = ACPConversationPort({'backend': 'vibe', 'version': '3.1.0', 'cwd': '/repo', 'mode': 'ask',
                            'model': 'mistral-medium-3.5', 'effort': 'high', 'print_trust_flag': True})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {'protocolVersion': 1, 'agentCapabilities': {'sessionCapabilities': {'close': {}}}})
    peer.answer(peer.frames()[0], {'sessionId': 'provisional', '_meta': {
        'workspace_trust': {'status': 'untrusted', 'details': {'cwd': '/repo'}}}})
    assert p.unknown.startswith('workspace_trust_unqualified') and not p.ready
    assert peer.frames() == []


def test_vibe_resume_trust_reload_cannot_substitute_fresh_context():
    p = ACPConversationPort({'backend': 'vibe', 'version': '3.1.0', 'cwd': '/repo', 'mode': 'ask',
                            'model': 'mistral-medium-3.5', 'effort': 'high', 'print_trust_flag': True, 'session_id': 'retained'})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {'protocolVersion': 1, 'agentCapabilities': {'sessionCapabilities': {'close': {}}}})
    load, = peer.frames()
    assert load['method'] == 'session/load' and load['params']['sessionId'] == 'retained'
    peer.answer(load, {'sessionId': 'unexpected-fresh', '_meta': {'workspace_trust': {'status': 'trusted'}}})
    assert p.unknown and not p.ready and not peer.frames()


def test_vibe_unrequested_workspace_trust_refuses_without_persistent_write():
    p = ACPConversationPort({'backend': 'vibe', 'version': '3.1.0', 'cwd': '/repo', 'mode': 'ask'})
    peer = Peer(p)
    peer.answer(peer.frames()[0], {'protocolVersion': 1, 'agentCapabilities': {'sessionCapabilities': {'close': {}}}})
    peer.answer(peer.frames()[0], {'sessionId': 'provisional', '_meta': {'workspace_trust': {'status': 'untrusted', 'details': {'cwd': '/repo'}}}})
    assert p.unknown and not p.ready and not peer.frames()


@pytest.mark.parametrize('expected', ['2.1.318', '2.1.319'])
def test_claude_init_version_remains_exact_for_supported_candidates(expected):
    p = ClaudeConversationPort({'version': expected, 'session_id': 'session-a'})
    peer = Peer(p)
    peer.send({'type': 'system', 'subtype': 'init', 'session_id': 'session-a', 'claude_code_version': expected})
    assert p.unknown is None
    peer.send({'type': 'system', 'subtype': 'init', 'session_id': 'session-a', 'claude_code_version': '2.1.320'})
    assert p.unknown == 'Claude native initialization version mismatch'


def test_acp_display_block_finishes_and_never_coalesces_logical_responses():
    from harflow_mcp.backends.base import Accumulator
    from harflow_mcp.backends.conversation import ConversationIdentity, ConversationObservation
    port = ACPConversationPort({'backend':'antigravity','cwd':'/repo','mode':'agent'})
    blocks = []
    for generation in (1, 2):
        identity = ConversationIdentity('session-a', f'native-{generation}', generation, f'logical-{generation}')
        acc = Accumulator()
        chunks = []
        for text in ('first ', 'second'):
            event = ConversationObservation('native_event', identity, {'frame':{'params':{'update':{
                'sessionUpdate':'agent_message_chunk','content':{'type':'text','text':text}}}}})
            chunks += port.normalize_observation(event, acc)
            acc.summary = (acc.summary or '') + text
        final = port.normalize_observation(ConversationObservation('response_terminal', identity, {}), acc)
        assert [event['text'] for event in chunks] == ['first ', 'second']
        assert final[0]['kind'] == 'assistant_text'  # visible without opting into streaming deltas
        assert final[0]['text'] == 'first second'
        assert {event['message_id'] for event in chunks + final} == {final[0]['message_id']}
        assert {event['block_index'] for event in chunks + final} == {0}
        blocks.append(final[0]['message_id'])
    assert blocks[0] != blocks[1]


def test_codex_delta_and_completed_item_share_exact_native_display_identity():
    from harflow_mcp.backends.base import Accumulator
    p, peer = codex()
    start_codex(p, peer)
    acc = Accumulator()
    events = []
    for frame in (
        {'method':'item/agentMessage/delta','params':{'threadId':'thread-a','turnId':'native-1',
            'itemId':'native-message','delta':'hello'}},
        {'method':'item/completed','params':{'threadId':'thread-a','turnId':'native-1',
            'item':{'type':'agentMessage','id':'native-message','text':'hello'}}}):
        events += [event for observation in peer.send(frame) for event in p.normalize_observation(observation, acc)]
    assert [(event['kind'],event['text']) for event in events] == [('assistant_delta','hello'),('assistant_text','hello')]
    assert {event['message_id'] for event in events} == {'native-message'}
    assert {event['block_index'] for event in events} == {0}
    missing = {'method':'item/agentMessage/delta','params':{'threadId':'thread-a','turnId':'native-1','delta':'unknown'}}
    assert not [event for observation in peer.send(missing) for event in p.normalize_observation(observation, acc)]


def test_claude_result_uuid_replay_after_fresh_next_message_cannot_settle():
    p, peer = claude()
    old_result = {'type':'result','uuid':'terminal-old','session_id':'session-a','is_error':False}
    for generation in (1, 2):
        p.issue('input', f'logical-{generation}', receive_fence=peer.sequence)
        peer.frames()
        peer.send({'type':'stream_event','session_id':'session-a','event':{
            'type':'message_start','message':{'id':f'assistant-{generation}'}}})
        if generation == 1:
            peer.send(old_result)
            assert p.can_issue
    observations = peer.send(old_result)
    assert all(event.kind == 'historical_frame' for event in observations)
    assert p.identity.logical_turn == 'logical-2'
    assert p.terminal is None and not p.can_issue
    peer.send({**old_result, 'uuid':'terminal-new'})
    assert p.can_issue


def test_claude_result_without_uuid_is_ambiguous_even_after_fresh_start():
    p, peer = claude()
    p.issue('input', 'logical', receive_fence=peer.sequence)
    peer.frames()
    peer.send({'type':'stream_event','session_id':'session-a','event':{
        'type':'message_start','message':{'id':'fresh-assistant'}}})
    peer.send({'type':'result','session_id':'session-a','is_error':False})
    assert p.unknown and p.terminal is None and not p.can_issue


@pytest.mark.parametrize('item_type', ['webSearch','imageView','collabAgentToolCall','subAgentActivity',
                                       'sleep','imageGeneration','futureNativeOperation'])
def test_codex_all_native_operations_block_boundary_until_exact_completion(item_type):
    p, peer = codex()
    start_codex(p, peer)
    item = {'type':item_type,'id':'native-operation'}
    peer.send({'method':'item/started','params':{'threadId':'thread-a','turnId':'native-1','item':item}})
    assert p.pending_tools == {'native-operation'}
    peer.send({'method':'turn/completed','params':{'threadId':'thread-a','turn':{'id':'native-1','status':'completed'}}})
    assert not p.can_issue
    peer.send({'method':'item/completed','params':{'threadId':'thread-a','turnId':'native-1','item':item}})
    assert not p.pending_tools and p.can_issue


@pytest.mark.parametrize('retained_old_root', [False, True])
def test_codex_resume_replaces_runtime_roots_without_inheriting_old_scratch(retained_old_root):
    roots = ['/repo', '/configured-cache', '/new-child-scratch', '/shared-chain']
    profile = {'backend':'codex','version':'0.166.0','cwd':'/repo','sandbox':'workspace-write',
        'approval_policy':'never','network':False,'writable_roots':roots,
        'runtime_workspace_roots':roots,'session_id':'original-thread',
        'config':{'sandbox_workspace_write':{'writable_roots':roots[1:], 'network_access':False,
                    'exclude_slash_tmp':True,'exclude_tmpdir_env_var':False}}}
    port = CodexConversationPort(profile)
    peer = Peer(port)
    initialize, = peer.frames()
    peer.answer(initialize, {'userAgent':'codex/0.166.0'})
    _, resume = peer.frames()
    assert resume['method'] == 'thread/resume'
    assert resume['params']['threadId'] == 'original-thread'
    assert resume['params']['runtimeWorkspaceRoots'] == roots
    assert resume['params']['config'] == profile['config']
    echoed = roots[1:] + (['/old-source-scratch'] if retained_old_root else [])
    observations = peer.answer(resume, {'thread':{'id':'original-thread'}, 'approvalPolicy':'never',
        'approvalsReviewer':'user','runtimeWorkspaceRoots':roots,
        'sandbox':{'type':'workspaceWrite','writableRoots':echoed,'networkAccess':False}})
    assert port.ready is (not retained_old_root)
    assert bool(port.unknown) is retained_old_root
    assert not peer.frames()  # No model prompt has been sent by initialization.
    if retained_old_root:
        assert not any(event.kind == 'session_ready' for event in observations)
