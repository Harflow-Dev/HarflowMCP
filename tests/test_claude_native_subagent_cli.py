"""Native Claude certification against a localhost fake API; never calls a model."""
import http.server
import json
import os
import shutil
import subprocess
import threading
import uuid
from contextlib import contextmanager

import pytest

from polybridge.backends.claude import ClaudeBackend
from polybridge.backends.claude_native import ClaudeNativeAdapter, PROFILE, PROFILE_NAME

pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(not os.environ.get("PB_CLI_INTEGRATION"), reason="opt in with PB_CLI_INTEGRATION=1"),
]


@contextmanager
def fake_api(repo, batch=False):
    seen_children = set()
    gate = threading.Event()
    requests = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path.endswith("count_tokens"):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b'{"input_tokens":100}')
                return
            requests.append(body)
            child = PROFILE[PROFILE_NAME]["prompt"] in json.dumps(body.get("system", []))
            results = [block for message in body["messages"] for block in message.get("content", [])
                       if isinstance(block, dict) and block.get("type") == "tool_result"]
            parent_assignment = next((block["text"] for message in reversed(body["messages"])
                                      if message["role"] == "user" for block in message.get("content", [])
                                      if isinstance(block, dict) and block.get("type") == "text"
                                      and "Agent arguments:\n" in block.get("text", "")), None)
            agent_arguments = json.loads(parent_assignment.split("Agent arguments:\n", 1)[1]) if parent_assignment else None
            parent_tool_id = "toolu_" + agent_arguments["description"] if isinstance(agent_arguments, dict) else None
            if batch and child:
                seen_children.add("first" if "SENTINEL first" in json.dumps(body) else "second")
                if len(seen_children) == 2:
                    gate.set()
                assert gate.wait(10), "Claude foreground children did not overlap"
            if child and not results:
                blocks = [
                    {"type": "tool_use", "id": "child-write", "name": "Write", "input": {"file_path": str(repo / "forbidden"), "content": "bad"}},
                    {"type": "tool_use", "id": "child-edit", "name": "Edit", "input": {"file_path": str(repo / "input.txt"), "old_string": "fixture", "new_string": "bad"}},
                    {"type": "tool_use", "id": "child-bash", "name": "Bash", "input": {"command": "touch forbidden-shell"}},
                    {"type": "tool_use", "id": "child-outside", "name": "Write", "input": {"file_path": str(repo.parent / (repo.name + "-outside")), "content": "forbidden"}},
                    {"type": "tool_use", "id": "child-read", "name": "Read", "input": {"file_path": str(repo / "input.txt")}},
                ]
            elif child:
                blocks = [{"type": "text", "text": "CHILD_RESULT_SENTINEL"}]
            elif isinstance(agent_arguments, list) and not any(block.get("tool_use_id", "").startswith("toolu_batch") for block in results):
                blocks = [{"type": "tool_use", "id": "toolu_" + a["description"], "name": "Agent", "input": a} for a in agent_arguments]
            elif isinstance(agent_arguments, dict) and not any(block.get("tool_use_id") == parent_tool_id for block in results):
                blocks = [{"type": "tool_use", "id": parent_tool_id, "name": "Agent", "input": agent_arguments}]
            else:
                blocks = [{"type": "text", "text": json.dumps({"native_dispatch_nonces": [a["description"] for a in agent_arguments], "settled": True}) if isinstance(agent_arguments, list) else "PARENT_RESULT_SENTINEL"}]
            events = [("message_start", {"type": "message_start", "message": {
                "id": f"msg_{len(requests)}", "type": "message", "role": "assistant", "content": [],
                "model": "claude-sonnet-4-6", "stop_reason": None, "stop_sequence": None,
                "usage": {"input_tokens": 100, "output_tokens": 0},
            }})]
            for index, block in enumerate(blocks):
                initial = dict(block)
                if block["type"] == "text":
                    initial["text"] = ""
                    delta = {"type": "text_delta", "text": block["text"]}
                else:
                    initial["input"] = {}
                    delta = {"type": "input_json_delta", "partial_json": json.dumps(block["input"])}
                events.extend([
                    ("content_block_start", {"type": "content_block_start", "index": index, "content_block": initial}),
                    ("content_block_delta", {"type": "content_block_delta", "index": index, "delta": delta}),
                    ("content_block_stop", {"type": "content_block_stop", "index": index}),
                ])
            events.extend([
                ("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use" if blocks[0]["type"] == "tool_use" else "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 20}}),
                ("message_stop", {"type": "message_stop"}),
            ])
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for name, event in events:
                self.wfile.write(f"event: {name}\ndata: {json.dumps(event)}\n\n".encode())
                self.wfile.flush()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("max_turns,child_cap,freedom", [(None, None, "read_only"), (100, None, "read_only"), (100, 1, "read_only"), (100, None, "write_in_repo")])
def test_real_claude_read_only_native_child_activity_eof_and_resume(tmp_path, max_turns, child_cap, freedom):
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if not version.startswith("2.1.295 "):
        pytest.skip(f"native certification is pinned to Claude 2.1.295; found {version}")
    (tmp_path / "input.txt").write_text("fixture")
    session = str(uuid.uuid4())
    with fake_api(tmp_path) as (url, requests):
        env = {key: value for key, value in os.environ.items() if not key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) and key != "CLAUDECODE"}
        env.update(HOME=str(tmp_path / "home"), CLAUDE_CONFIG_DIR=str(tmp_path / "config"),
                   ANTHROPIC_API_KEY="fake-local-key", ANTHROPIC_BASE_URL=url,
                   CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1")
        backend = ClaudeBackend()
        adapter = ClaudeNativeAdapter()
        def run(resume, nonce):
            builder = backend.build_resume_argv if resume else backend.build_start_argv
            invocation = adapter.configure(builder(
                adapter.prompt("Return CHILD_RESULT_SENTINEL", nonce), repo=tmp_path,
                freedom=freedom, session_id=session, model="claude-sonnet-4-6",
                max_turns=max_turns, reasoning_effort=None,
            ), {"freedom": freedom})
            backend.assert_safe(invocation, freedom)
            assert json.loads(invocation.argv[invocation.argv.index("--agents") + 1])[PROFILE_NAME]["maxTurns"] == 100
            if max_turns:
                assert invocation.argv[invocation.argv.index("--max-turns") + 1] == "100"
            if child_cap is not None:
                # Deliberately probe a stricter cap separately from exact production certification.
                profile = json.loads(invocation.argv[invocation.argv.index("--agents") + 1])
                profile[PROFILE_NAME]["maxTurns"] = child_cap
                invocation.argv[invocation.argv.index("--agents") + 1] = json.dumps(profile)
            assert invocation.stdin_mode == ("devnull" if max_turns else "pipe")
            result = subprocess.run(invocation.argv, input=invocation.initial_input,
                                    cwd=tmp_path, env=env, capture_output=True, timeout=40)
            assert result.returncode == 0, result.stderr.decode()
            return [json.loads(line) for line in result.stdout.decode().splitlines() if line.startswith("{")]
        def verify_adapter_observation(events, nonce):
            state = {"owner_session_id": session, "assignment": "Return CHILD_RESULT_SENTINEL", "owner_freedom": freedom}
            updates = [update for event in events for update in adapter.observe(event, nonce, state)]
            assert next(update for update in updates if update["native_update"] == "started")["native_child_id"]
            settled = next(update for update in updates if update["native_update"] == "settled")
            assert settled["status"] == "completed" and settled["summary"] == "CHILD_RESULT_SENTINEL"
            assert any(update["native_update"] == "activity" and update["event_kind"] == "tool_call" for update in updates)
            assert state["terminal"] is True

        events = run(False, "PB_NODE_NONCE_FIRST")
        if child_cap is not None:
            native = next(e["tool_use_result"] for e in events if isinstance(e.get("tool_use_result"), dict) and "agentId" in e["tool_use_result"])
            # The CLI calls exhausted native children completed. Their harness note is not a worker report.
            assert native["status"] == "completed"
            assert native["harnessNoteCount"] == 1
            assert "stopped at its 1-turn limit before finishing" in native["content"][0]["text"]
            assert not any("CHILD_RESULT_SENTINEL" in json.dumps(e.get("message", {}))
                           for e in events if e.get("type") == "assistant" and e.get("parent_tool_use_id"))
            child_requests = [r for r in requests if PROFILE[PROFILE_NAME]["prompt"] in json.dumps(r.get("system", []))]
            assert len(child_requests) == 1
            assert (tmp_path / "forbidden").exists() == (freedom == "write_in_repo")
            assert not (tmp_path / "forbidden-shell").exists()
            assert (tmp_path / "input.txt").read_text() == ("bad" if freedom == "write_in_repo" else "fixture")
            return
        verify_adapter_observation(events, "PB_NODE_NONCE_FIRST")
        (tmp_path / "native-events.jsonl").write_text("\n".join(json.dumps(e) for e in events))
        started = next(e for e in events if e.get("subtype") == "task_started")
        assert started["task_type"] == "local_agent"
        assert started["is_backgrounded"] is False
        assert started["tool_use_id"] == "toolu_PB_NODE_NONCE_FIRST"
        child_id = started["task_id"]
        terminal = next(e for e in events if e.get("subtype") == "task_notification")
        assert terminal["task_id"] == child_id and terminal["status"] == "completed"
        completed = next(e["tool_use_result"] for e in events if isinstance(e.get("tool_use_result"), dict) and "agentId" in e["tool_use_result"])
        assert completed["agentId"] == child_id and completed["status"] == "completed"
        assert completed["resolvedModel"] == "claude-sonnet-4-6"
        child_events = [e for e in events if e.get("parent_tool_use_id") == "toolu_PB_NODE_NONCE_FIRST"]
        assert "CHILD_RESULT_SENTINEL" in json.dumps(child_events)
        child_results = [b for e in child_events for b in e.get("message", {}).get("content", []) if b.get("type") == "tool_result"]
        for tool_id in ("child-write", "child-edit", "child-bash"):
            assert next(b for b in child_results if b["tool_use_id"] == tool_id).get("is_error", False) == (freedom == "read_only" or tool_id == "child-bash")
        assert next(b for b in child_results if b["tool_use_id"] == "child-outside").get("is_error", False)
        assert not (tmp_path.parent / (tmp_path.name + "-outside")).exists()
        assert not next(b for b in child_results if b["tool_use_id"] == "child-read").get("is_error", False)
        assert (tmp_path / "forbidden").exists() == (freedom == "write_in_repo") and not (tmp_path / "forbidden-shell").exists()
        assert (tmp_path / "input.txt").read_text() == ("bad" if freedom == "write_in_repo" else "fixture")
        child_requests = [r for r in requests if PROFILE[PROFILE_NAME]["prompt"] in json.dumps(r.get("system", []))]
        assert child_requests
        assert {t["name"] for t in child_requests[0]["tools"]} == ({"Read", "Glob", "Grep", "Write", "Edit"} if freedom == "write_in_repo" else {"Read", "Glob", "Grep"})
        assert events[-1]["type"] == "result" and events[-1]["is_error"] is False
        assert events[-1]["subagent_stats"]["completed"] == 1
        resumed = run(True, "PB_NODE_NONCE_SECOND")
        verify_adapter_observation(resumed, "PB_NODE_NONCE_SECOND")
        second_started = next(e for e in resumed if e.get("subtype") == "task_started")
        assert second_started["tool_use_id"] == "toolu_PB_NODE_NONCE_SECOND"
        assert second_started["task_id"] != child_id
        assert second_started["is_backgrounded"] is False
        second_completed = next(e["tool_use_result"] for e in resumed if isinstance(e.get("tool_use_result"), dict) and "agentId" in e["tool_use_result"])
        assert second_completed["agentId"] == second_started["task_id"]
        assert second_completed["status"] == "completed"
        assert "CHILD_RESULT_SENTINEL" in json.dumps(second_completed["content"])
        assert any(e.get("parent_tool_use_id") == "toolu_PB_NODE_NONCE_SECOND" for e in resumed)
        assert resumed[-1]["type"] == "result" and resumed[-1]["session_id"] == session
        assert resumed[-1]["is_error"] is False


def test_real_claude_parallel_foreground_batch(tmp_path):
    executable = shutil.which("claude")
    if not executable or not subprocess.run([executable, "--version"], capture_output=True, text=True).stdout.startswith("2.1.295 "):
        pytest.skip("Requires certified CLI")
    (tmp_path / "input.txt").write_text("fixture")
    entries = [{"assignment": "Return CHILD_RESULT_SENTINEL " + name, "nonce": "batch_" + name} for name in ["first", "second"]]
    session = str(uuid.uuid4())
    with fake_api(tmp_path, batch=True) as (url, requests):
        env = {key: value for key, value in os.environ.items() if not key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) and key != "CLAUDECODE"}
        env.update(HOME=str(tmp_path / "home"), CLAUDE_CONFIG_DIR=str(tmp_path / "config"), ANTHROPIC_API_KEY="fake-local-key", ANTHROPIC_BASE_URL=url, CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1")
        backend, adapter = ClaudeBackend(), ClaudeNativeAdapter()
        invocation = adapter.configure(backend.build_start_argv(adapter.prompt_batch(entries), repo=tmp_path, freedom="read_only", session_id=session, model="claude-sonnet-4-6", max_turns=100, reasoning_effort=None))
        backend.assert_safe(invocation, "read_only")
        result = subprocess.run(invocation.argv, input=invocation.initial_input, cwd=tmp_path, env=env, capture_output=True, timeout=35)
        assert result.returncode == 0, result.stderr.decode()
        events = [json.loads(line) for line in result.stdout.decode().splitlines() if line.startswith("{")]
        children = []
        for entry in entries:
            state = {"owner_session_id": session, "assignment": entry["assignment"], "batch_entries": entries}
            updates = [u for event in events for u in adapter.observe(event, entry["nonce"], state)]
            assert next(u for u in updates if u["native_update"] == "settled")["status"] == "completed"
            children.append(state["native_child_id"])
        assert len(set(children)) == 2
        assert not (tmp_path / "forbidden").exists()
