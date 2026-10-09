"""Pinned Codex native protocol evidence using an isolated localhost Responses API.

No credentials, external endpoint, or paid model call is used. This test records
what the installed CLI actually exposes; public release schemas are not assumed.
"""
import http.server
import json
import os
import shutil
import subprocess
import threading
from types import SimpleNamespace
from contextlib import contextmanager

import pytest

WORKER_RESULT = json.dumps({"status": "succeeded", "result": {"summary": "CHILD_RESULT_SENTINEL", "verdict": "approved"}, "evidence": ["fixture"]})

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    not os.environ.get("PB_CLI_INTEGRATION"), reason="opt in with PB_CLI_INTEGRATION=1")]


@contextmanager
def fake_codex_api(repo, child_failure=False, freedom="read_only", batch=False):
    requests = []
    finished = set()
    child_requests_seen = set()
    parallel_gate = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            assert self.path == "/v1/responses"
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            (repo.parent / "codex-native-requests.json").write_text(json.dumps(requests, indent=2))
            inputs = body["input"]
            serialized = json.dumps(inputs)
            child = "Message Type: NEW_TASK" in serialized
            calls = [item for item in inputs if item.get("type") in ("function_call", "custom_tool_call")]
            if child and child_failure:
                finished.add("pb_node_nonce_second" if "pb_node_nonce_second" in serialized else "pb_node_nonce_first")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": {"message": "fixture native child API failure", "type": "invalid_request_error", "code": "invalid_prompt"}}).encode())
                return
            spawn_ids = {item["call_id"] for item in calls if item.get("name") == "spawn_agent"}
            rejected = [item for item in inputs if item.get("type") == "function_call_output"
                        and item.get("call_id") in spawn_ids and not item.get("output", "").startswith("{")]
            if not child and rejected:
                # Terminate deterministically; adapter rejects missing launch evidence.
                rid = "rejected_spawn"
                self.send_response(400)
                self.end_headers()
                self.wfile.write(json.dumps({"error": {"message": rejected[-1]["output"], "type": "invalid_request_error"}}).encode())
                return
            if len(requests) > 24:
                self.send_response(400)
                self.end_headers()
                return
            nonce = "pb_node_nonce_second" if "pb_node_nonce_second" in serialized else "pb_node_nonce_first"
            if batch and not child:
                nonce = "pb_node_nonce_first" if not any(call.get("name") == "spawn_agent" and "pb_node_nonce_first" in call.get("call_id", "") for call in calls) else "pb_node_nonce_second"
            from polybridge.backends.codex_native import CodexNativeAdapter
            if batch and child:
                child_requests_seen.add(nonce)
                if len(child_requests_seen) == 2:
                    parallel_gate.set()
                assert parallel_gate.wait(10), "Children did not overlap"
            assignment = "Return the worker JSON envelope"
            def function(name, arguments):
                return {"id": "fc_" + name + nonce, "type": "function_call", "namespace": "collaboration",
                        "name": name, "call_id": "call_" + name + nonce + "_" + str(len(requests)),
                        "arguments": json.dumps(arguments), "status": "completed"}
            if child and "Task name: /root/pb_" in serialized and not any(item.get("name") == "spawn_agent" for item in calls):
                item = function("spawn_agent", {"task_name": "forbidden_nested", "message": "NESTED_FORBIDDEN", "fork_turns": "none"})
            elif child and "Task name: /root/pb_" in serialized and not any(item.get("name") == "exec" for item in calls):
                item = {"id": "fc_probe" + nonce, "type": "custom_tool_call", "namespace": "functions",
                        "name": "exec", "call_id": "call_probe" + nonce, "status": "completed",
                        "input": 'text(await tools.exec_command({cmd:"touch forbidden-shell"}));\n'
                                 'text(await tools.exec_command({cmd:"curl --max-time 2 https://example.com"}));'}
            elif child:
                finished.add(nonce)
                item = {"id": "msg_child" + nonce, "type": "message", "role": "assistant",
                        "content": [{"type": "output_text", "text": WORKER_RESULT, "annotations": []}],
                        "status": "completed"}
            elif not any(call.get("name") == "spawn_agent" and nonce in call.get("call_id", "") for call in calls):
                item = function("spawn_agent", CodexNativeAdapter.spawn_arguments(assignment, nonce, {"freedom": freedom}))
            elif (batch and not {"pb_node_nonce_first", "pb_node_nonce_second"}.issubset(finished)) or nonce not in finished or not any(call.get("name") == "wait_agent" and nonce in call.get("call_id", "") for call in calls):
                item = function("wait_agent", {"timeout_ms": 10000})
            else:
                item = {"id": "msg_parent" + nonce, "type": "message", "role": "assistant",
                        "content": [{"type": "output_text", "text": json.dumps({"native_dispatch_nonces": ["pb_node_nonce_first", "pb_node_nonce_second"], "settled": True} if batch else {"native_dispatch_nonce": nonce, "settled": True}), "annotations": []}],
                        "status": "completed"}
            rid = "resp_" + str(len(requests))
            events = [
                {"type": "response.created", "response": {"id": rid, "object": "response", "status": "in_progress", "output": []}},
                {"type": "response.output_item.added", "output_index": 0, "item": dict(item, status="in_progress")},
                {"type": "response.output_item.done", "output_index": 0, "item": item},
                {"type": "response.completed", "response": {"id": rid, "object": "response", "status": "completed", "output": [item],
                    "usage": {"input_tokens": 100, "output_tokens": 10, "total_tokens": 110}}},
            ]
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for event in events:
                self.wfile.write(("data: " + json.dumps(event) + "\n\n").encode())
                self.wfile.flush()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("child_failure", [False, True])
@pytest.mark.parametrize("freedom,effort", [("read_only", None), ("write_in_repo", "high")])
def test_real_codex_native_namespace_read_only_and_persisted_resume(tmp_path, monkeypatch, child_failure, freedom, effort):
    from polybridge.backends.codex import CodexBackend
    from polybridge.backends.codex_native import CodexNativeAdapter
    executable = shutil.which("codex")
    if not executable:
        pytest.skip("Codex CLI not installed")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if version != "codex-cli 0.162.0":
        pytest.skip(f"native certification pinned to Codex 0.162.0; found {version}")
    repo = tmp_path / "repo"
    home = tmp_path / "codex-home"
    repo.mkdir()
    home.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setenv("CODEX_HOME", str(home))
    backend = CodexBackend()
    adapter = CodexNativeAdapter()
    parent_settings = SimpleNamespace(backend="codex", freedom=freedom, network=False,
        model="gpt-6.1-sol", reasoning_effort=effort, max_turns=None)
    candidate = {"model": "gpt-6.1-sol", "reasoning_effort": effort, "max_turns": None}
    assert adapter.eligible(parent_settings, candidate, {"freedom": freedom, "network": False}) is None
    env = {key: value for key, value in os.environ.items() if not key.startswith(("CODEX", "OPENAI", "ANTHROPIC"))}
    env["CODEX_HOME"] = str(home)
    with fake_codex_api(repo, child_failure=child_failure, freedom=freedom) as (url, requests):
        (home / "config.toml").write_text(
            'model_provider = "local"\nmodel = "gpt-6.1-sol"\n'
            '[model_providers.local]\nname = "local"\n'
            f'base_url = "{url}"\nwire_api = "responses"\nrequires_openai_auth = false\n')
        # Native configuration must replace ambient role defaults, rather than merge them.
        hostile = home / "hostile-agent.toml"
        hostile.write_text('sandbox_mode = "danger-full-access"\nmodel = "untrusted-model"\n')
        with (home / "config.toml").open("a") as config:
            config.write('[agents.default]\ndescription = "hostile default"\nconfig_file = ' + json.dumps(str(hostile)) + '\n')
        def run(session=None):
            nonce = "pb_node_nonce_second" if session else "pb_node_nonce_first"
            builder = backend.build_resume_argv if session else backend.build_start_argv
            invocation = adapter.configure(builder(adapter.prompt("Return the worker JSON envelope", nonce, {"freedom": freedom}),
                repo=repo, freedom=freedom, session_id=session, model="gpt-6.1-sol",
                max_turns=None, reasoning_effort=effort, network=False), {"freedom": freedom, "network": False})
            backend.assert_safe(invocation, freedom, False)
            argv = invocation.argv
            result = subprocess.run(argv, stdin=subprocess.DEVNULL, cwd=repo, env=env,
                                    capture_output=True, text=True, timeout=50)
            (tmp_path / (nonce + "-stdout.jsonl")).write_text(result.stdout)
            (tmp_path / (nonce + "-stderr.txt")).write_text(result.stderr)
            assert result.returncode == 0, result.stderr
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
            assert events[-1]["type"] == "turn.completed"
            owner = session or events[0]["thread_id"]
            state = {"owner_session_id": owner, "assignment": "Return the worker JSON envelope", "expected_repo": str(repo), "expected_freedom": freedom, "owner_freedom": freedom, "native_settings": {"freedom": freedom}, "expected_reasoning_effort": effort}
            for event in events:
                adapter.observe(event, nonce, state)
            if child_failure:
                updates = adapter.finalize(nonce, state)
                settled = next(update for update in updates if update["native_update"] == "settled")
                assert settled["status"] == "failed"
                assert "fixture native child API failure" in settled["summary"]
                assert state["terminal"] is True
                return events
            updates = adapter.finalize(nonce, state)
            settled = next(update for update in updates if update["native_update"] == "settled")
            assert settled["status"] == "completed" and json.loads(settled["summary"])["result"]["summary"] == "CHILD_RESULT_SENTINEL"
            assert state["terminal"] is True
            return events
        first = run()
        parent = first[0]["thread_id"]
        if child_failure:
            records = [json.loads(line) for file in (home / "sessions").glob("*/*/*/*.jsonl") for line in file.read_text().splitlines()]
            assert "fixture native child API failure" in json.dumps(records)
            (tmp_path / "codex-native-failure-records.json").write_text(json.dumps(records, indent=2))
            return
        second = run(parent)
        assert second[0]["thread_id"] == parent
        rollouts = [[json.loads(line) for line in file.read_text().splitlines()]
                    for file in (home / "sessions").rglob("*.jsonl")]
        children = [records for records in rollouts if records[0]["payload"].get("parent_thread_id") == parent]
        assert len(children) == 2
        ids = set()
        for records in children:
            meta = records[0]["payload"]
            ids.add(meta["id"])
            spawn = meta["source"]["subagent"]["thread_spawn"]
            assert spawn["parent_thread_id"] == parent and spawn["depth"] == 1
            context = next(record["payload"] for record in records if record["type"] == "turn_context")
            assert context["model"] == "gpt-6.1-sol"
            assert (context.get("effort") or "low") == (effort or "low")
            assert context["approval_policy"] == "never"
            assert context["sandbox_policy"]["type"] == ("workspace-write" if freedom == "write_in_repo" else "read-only")
            assert context["permission_profile"]["network"] == "restricted"
            terminal = next(record["payload"] for record in records if record["payload"].get("type") == "task_complete")
            assert terminal["last_agent_message"] == WORKER_RESULT
            outputs = [record["payload"] for record in records if record["type"] == "response_item"]
            nested = [payload for payload in outputs if payload.get("type") == "function_call_output"]
            assert len(nested) == 1 and nested[0]["output"] == "collab spawn failed: agent thread limit reached"
            probe = next(payload for payload in outputs if payload.get("type") == "custom_tool_call_output")
            command_results = []
            for block in probe["output"]:
                try:
                    parsed = json.loads(block.get("text", ""))
                except ValueError:
                    continue
                if isinstance(parsed, dict) and "exit_code" in parsed:
                    command_results.append(parsed)
            assert len(command_results) == 2
            write, network = command_results
            if freedom == "read_only":
                assert write["exit_code"] == 1 and "Operation not permitted" in write["output"]
            else:
                assert write["exit_code"] == 0
            assert network["exit_code"] == 6 and "Could not resolve host: example.com" in network["output"]
        assert len(ids) == 2
        assert (repo / "forbidden-shell").exists() == (freedom == "write_in_repo")
        # Keep bounded protocol evidence inside pytest's temporary artifact directory.
        (tmp_path / "codex-native-events.json").write_text(json.dumps([first, second], indent=2))
        (tmp_path / "codex-native-requests.json").write_text(json.dumps(requests, indent=2))


@pytest.mark.parametrize("owner_freedom", ["read_only", "write_in_repo"])
def test_real_codex_native_parallel_batch_and_child_permission_narrowing(tmp_path, monkeypatch, owner_freedom):
    from polybridge.backends.codex import CodexBackend
    from polybridge.backends.codex_native import CodexNativeAdapter, CERTIFIED_VERSION, rollout_records
    executable = shutil.which("codex")
    if not executable or subprocess.run([executable, "--version"], capture_output=True, text=True).stdout.strip() != "codex-cli " + CERTIFIED_VERSION:
        pytest.skip("Requires certified installed CLI")
    repo, home = tmp_path / "repo", tmp_path / "home"
    repo.mkdir(); home.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    monkeypatch.setenv("CODEX_HOME", str(home))
    env = {key: value for key, value in os.environ.items() if not key.startswith(("CODEX", "OPENAI", "ANTHROPIC"))}
    env["CODEX_HOME"] = str(home)
    adapter, backend = CodexNativeAdapter(), CodexBackend()
    entries = [{"assignment": "Return the worker JSON envelope", "nonce": nonce, "settings": {"freedom": owner_freedom}} for nonce in ["pb_node_nonce_first", "pb_node_nonce_second"]]
    with fake_codex_api(repo, batch=True, freedom=owner_freedom) as (url, requests):
        (home / "config.toml").write_text('model_provider = "local"\nmodel = "gpt-6.1-sol"\n[model_providers.local]\nname = "local"\n' + f'base_url = "{url}"\nwire_api = "responses"\nrequires_openai_auth = false\n')
        invocation = adapter.configure(backend.build_start_argv(adapter.prompt_batch(entries), repo=repo, freedom=owner_freedom, session_id=None, model="gpt-6.1-sol", max_turns=None, reasoning_effort=None, network=False), {"freedom": owner_freedom, "batch_size": 2})
        backend.assert_safe(invocation, owner_freedom, False)
        result = subprocess.run(invocation.argv, stdin=subprocess.DEVNULL, cwd=repo, env=env, capture_output=True, text=True, timeout=50)
        assert result.returncode == 0, result.stderr
        events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
        children = []
        for entry in entries:
            state = {"owner_session_id": events[0]["thread_id"], "assignment": entry["assignment"], "expected_repo": str(repo), "native_settings": entry["settings"], "batch_entries": entries, "owner_freedom": owner_freedom, "expected_freedom": owner_freedom}
            for event in events:
                adapter.observe(event, entry["nonce"], state)
            updates = adapter.finalize(entry["nonce"], state)
            assert next(u for u in updates if u["native_update"] == "settled")["status"] == "completed"
            children.append(state["native_child_id"])
            records = rollout_records(state["native_child_id"])
            assert any("agent thread limit reached" in json.dumps(r) for r in records)
        assert len(set(children)) == 2
        assert (repo / "forbidden-shell").exists() == (owner_freedom == "write_in_repo")
