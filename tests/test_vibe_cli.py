"""Installed Vibe 3 qualification against a loopback-only scripted provider; no paid calls."""
import http.server
import json
import os
import shutil
import subprocess
import threading
from contextlib import contextmanager

import pytest

from harflow_mcp.backends.base import Accumulator
from harflow_mcp.backends.vibe import VibeBackend

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    os.environ.get("HARFLOW_CLI_INTEGRATION") != "1", reason="requires installed Vibe 3 and local listener")]


@contextmanager
def provider(mode):
    requests = []
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            requests.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            if mode == "limit":
                self.send_response(402)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":{"message":"Usage limit exceeded","type":"usage_limit"}}')
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            tool = mode == "cap" or (mode in ("deny", "allow") and len(requests) == 1)
            delta = {"role": "assistant", "content": "LOCAL ANSWER"}
            if tool:
                delta = {"role": "assistant", "tool_calls": [{"index": 0, "id": "local-call", "type": "function", "function": {"name": "bash", "arguments": json.dumps({"command": "touch denied-file"})}}]}
            for part, finish in [(delta, None), ({}, "tool_calls" if tool else "stop")]:
                chunk = {"id": "local", "object": "chat.completion.chunk", "created": 1, "model": "local", "choices": [{"index": 0, "delta": part, "finish_reason": finish}]}
                self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
            self.wfile.write(b"data: [DONE]\n\n")
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("mode", ["text", "deny", "allow", "cap", "limit"])
def test_vibe3_actual_stream(tmp_path, mode):
    binary = shutil.which("vibe")
    if not binary:
        pytest.skip("Vibe not installed")
    version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=10)
    if "3.0.1" not in version.stdout:
        pytest.skip("qualified against Vibe 3.0.1")
    home = tmp_path / "home"
    config = tmp_path / "vibe"
    repo = tmp_path / "repo"
    for directory in (home, config, repo):
        directory.mkdir()
    # Allowlist the child environment: no inherited provider credentials or overrides.
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "TMPDIR", "SYSTEMROOT", "LANG")}
    env.update(HOME=str(home), VIBE_HOME=str(config), VIBE_CLI="python", DO_NOT_TRACK="1")
    backend = VibeBackend()
    with provider(mode) as (url, requests):
        (config / "config.toml").write_text(f'''active_model="local"
api_retry_max_elapsed_time=0.1
[[providers]]
name="local"
api_base="{url}"
api_key_env_var=""
[[models]]
name="local"
alias="local"
provider="local"
thinking="off"
''')
        if mode == "allow":
            with (config / "config.toml").open("a") as settings:
                settings.write('\n[tools.bash]\npermission="always"\n')
        def run(prompt, session=None):
            method = backend.build_resume_argv if session else backend.build_start_argv
            invocation = method(prompt, repo=repo, freedom="read_only", session_id=session, model=None, max_turns=1 if mode == "cap" else 3, reasoning_effort=None)
            backend.assert_safe(invocation, "read_only")
            process = subprocess.run([binary, *invocation.argv[1:]], env=env, input=invocation.initial_input or b"", capture_output=True, timeout=30)
            acc = Accumulator()
            entries = [json.loads(line) for line in process.stdout.splitlines()]
            for entry in entries:
                backend.ingest(entry, acc)
                backend.normalize(entry, acc)
            return process, acc, entries
        process, acc, entries = run("isolated assignment")
        assert requests
        assert acc.session_id
        if mode == "text":
            assert process.returncode == 0, process.stderr
            assert backend.classify(acc, process.returncode) == "completed"
            assert acc.summary == "LOCAL ANSWER"
            resumed, next_acc, _ = run("resume assignment", acc.session_id)
            assert resumed.returncode == 0, resumed.stderr
            assert next_acc.session_id == acc.session_id
            assert next_acc.summary == "LOCAL ANSWER"
            assert any(message.get("content") == "resume assignment" for message in requests[-1]["messages"])
            large = "LARGE" + "x" * 40000
            piped, large_acc, large_entries = run(large)
            assert piped.returncode == 0, piped.stderr
            assert large_entries[0]["content"][0]["text"] == large
            assert large_acc.summary == "LOCAL ANSWER"
        elif mode == "allow":
            assert process.returncode == 0, process.stderr
            assert (repo / "denied-file").exists()
            assert not acc.denials
        elif mode == "deny":
            assert acc.denials
            assert process.returncode == 0, process.stderr
            assert backend.classify(acc, process.returncode) == "completed"
            assert not (repo / "denied-file").exists()
        else:
            assert process.returncode != 0
            assert backend.classify(acc, process.returncode) == "failed"
            assert backend.continuation_reason(acc, process.returncode) is None
            if mode == "cap":
                assert b"turn_limit" in process.stderr
                assert not acc.saw_final_message
            else:
                assert b"Usage limit exceeded" in process.stderr
