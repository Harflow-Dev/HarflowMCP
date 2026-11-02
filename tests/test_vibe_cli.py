"""Installed Vibe 3 qualification against a loopback-only scripted provider; no paid calls."""
import http.server
import json
import os
import shutil
import subprocess
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest

from harflow_mcp.backends.base import Accumulator
from harflow_mcp.backends.vibe import VibeBackend

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    os.environ.get("HARFLOW_CLI_INTEGRATION") != "1", reason="requires installed Vibe 3 and local listener")]


@contextmanager
def provider(mode, tool_call=None):
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
            tool = mode == "cap" or ((mode in ("deny", "allow") or tool_call) and len(requests) == 1)
            delta = {"role": "assistant", "content": "LOCAL ANSWER"}
            if tool:
                delta = {"role": "assistant", "tool_calls": [{"index": 0, "id": "local-call", "type": "function", "function": {"name": tool_call[0] if tool_call else "bash", "arguments": json.dumps(tool_call[1] if tool_call else {"command": "touch denied-file"})}}]}
            for part, finish in [(delta, None), ({}, "tool_calls" if tool else "stop")]:
                chunk = {"id": "local", "object": "chat.completion.chunk", "created": 1, "model": "local", "choices": [{"index": 0, "delta": part, "finish_reason": finish}]}
                self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
            self.wfile.write(("data: " + json.dumps({"id": "local", "object": "chat.completion.chunk", "created": 1, "model": "local", "choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}}) + "\n\n").encode())
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
    if version.stdout.strip() not in ("vibe 3.0.1", "vibe 3.0.3"):
        pytest.skip("qualified against Vibe 3.0.1 and 3.0.3")
    home = tmp_path / "home"
    config = tmp_path / "vibe"
    repo = tmp_path / "repo"
    for directory in (home, config, repo):
        directory.mkdir()
    # Allowlist the child environment: no inherited provider credentials or overrides.
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "TMPDIR", "SYSTEMROOT", "LANG")}
    env.update(HOME=str(home), VIBE_HOME=str(config), VIBE_CLI="python", DO_NOT_TRACK="1", HARFLOW_LOCAL_TEST_KEY="local-only-placeholder", MISTRAL_API_KEY="local-only-placeholder")
    backend = VibeBackend()
    with provider(mode) as (url, requests):
        (config / "config.toml").write_text(f'''active_model="local"
api_retry_max_elapsed_time=0.1
[[providers]]
name="local"
api_base="{url}"
api_key_env_var="HARFLOW_LOCAL_TEST_KEY"
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
            for line in process.stderr.decode(errors="replace").splitlines():
                backend.observe_stderr(acc, line)
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
                assert acc.failure_diagnostic["category"] == "usage_limit"
                assert acc.failure_diagnostic["http_status"] == 402


@pytest.mark.parametrize("case", [
    "policy-never", "bash-denylist", "standalone-denylist",
    "command-pattern", "outside-directory", "file-pattern",
])
def test_vibe303_refusal_shapes(tmp_path, case):
    """Measure real runtime refusals; the provider cannot choose to skip the tool."""
    binary = shutil.which("vibe")
    if not binary:
        pytest.skip("Vibe not installed")
    version = subprocess.run([binary, "--version"], capture_output=True, text=True, timeout=10)
    if version.stdout.strip() != "vibe 3.0.3":
        pytest.skip("denial shapes qualified against Vibe 3.0.3")
    home, config, repo = (tmp_path / name for name in ("home", "vibe", "repo"))
    for directory in (home, config, repo):
        directory.mkdir()
    target = repo / "denied-file"
    tool_call = ("bash", {"command": "touch denied-file"})
    settings = ""
    if case == "policy-never":
        settings = '[tools.bash]\npermission="never"\n'
    elif case == "bash-denylist":
        settings = '[tools.bash]\ndenylist=["touch"]\n'
    elif case == "standalone-denylist":
        settings = '[tools.bash]\ndenylist_standalone=["touch"]\n'
        tool_call = ("bash", {"command": "touch"})
    elif case == "outside-directory":
        target = tmp_path / "outside-file"
        tool_call = ("write_file", {"path": str(target), "content": "blocked"})
    elif case == "file-pattern":
        target = repo / ".env"
        tool_call = ("write_file", {"path": str(target), "content": "blocked"})
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "TMPDIR", "SYSTEMROOT", "LANG")}
    env.update(HOME=str(home), VIBE_HOME=str(config), VIBE_CLI="python", DO_NOT_TRACK="1", HARFLOW_LOCAL_TEST_KEY="local-only-placeholder", MISTRAL_API_KEY="local-only-placeholder")
    backend = VibeBackend()
    with provider("denial-shape", tool_call) as (url, requests):
        (config / "config.toml").write_text(f'''active_model="local"
api_retry_max_elapsed_time=0.1
[[providers]]
name="local"
api_base="{url}"
api_key_env_var="HARFLOW_LOCAL_TEST_KEY"
[[models]]
name="local"
alias="local"
provider="local"
thinking="off"
{settings}
''')
        invocation = backend.build_start_argv(
            "isolated denial measurement", repo=repo, freedom="read_only", session_id=None,
            model=None, max_turns=3, reasoning_effort=None)
        backend.assert_safe(invocation, "read_only")
        process = subprocess.run([binary, *invocation.argv[1:]], env=env,
                                 input=invocation.initial_input or b"", capture_output=True, timeout=30)
    assert process.returncode == 0, process.stderr
    assert len(requests) == 2
    assert not target.exists()
    entries = [json.loads(line) for line in process.stdout.splitlines()]
    fixture = json.loads((Path(__file__).parent / "fixtures" / "vibe-denials" / f"{case}.json").read_text())
    effects = [entry for entry in entries if entry.get("type") == "effect"]
    callbacks = [entry for entry in entries if entry.get("type") == "callback"]
    expected_effect = next(entry for entry in fixture["entries"] if entry["type"] == "effect")
    assert len(effects) == 1
    effect = effects[0]
    assert effect["state"]["status"] == "failed"
    assert effect["state"]["error"] == expected_effect["state"]["error"]
    assert effect["state"]["output"]["error"] == expected_effect["state"]["output"]["error"]
    assert effect["state"]["output"]["_meta"] == expected_effect["state"]["output"]["_meta"]
    assert effect["turnId"] == entries[0]["turnId"]
    assert effect["inputEntryId"] == entries[0]["id"]
    expected_callbacks = [entry for entry in fixture["entries"] if entry["type"] == "callback"]
    assert len(callbacks) == len(expected_callbacks)
    if callbacks:
        callback = callbacks[0]
        assert callback["relatedEntryId"] == effect["id"]
        assert callback["detail"]["relatedEntryId"] == effect["id"]
        assert callback["turnId"] == effect["turnId"]
        assert callback["inputEntryId"] == effect["inputEntryId"]
        assert callback["state"] == expected_callbacks[0]["state"]
        permissions = callback["detail"]["requiredPermissions"]
        assert len(permissions) == 1
        expected_permission = expected_callbacks[0]["detail"]["requiredPermissions"][0]
        assert permissions[0] == {
            key: value.replace("/fixture", str(tmp_path)) if isinstance(value, str) else value
            for key, value in expected_permission.items()
        }
    assert any(entry.get("role") == "assistant" for entry in entries)
