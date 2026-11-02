"""No-paid Vibe ACP command grant measurement against a localhost provider."""
import json
import http.server
from contextlib import contextmanager
import os
import queue
import shutil
import subprocess
import sys
import threading
import time

import pytest

from harflow_mcp.backends.base import Invocation
from harflow_mcp.backends import vibe_commands as grants

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    os.environ.get("HARFLOW_CLI_INTEGRATION") != "1" or sys.platform != "darwin", reason="requires installed Vibe and localhost listener")]


@contextmanager
def provider(mode, tool_call):
    requests = []
    prompted = set()

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            users = [message for message in body["messages"] if message["role"] == "user"]
            prompt = json.dumps(users[-1]["content"]) if users else ""
            use_tool = bool(body.get("tools")) and prompt not in prompted
            if use_tool:
                prompted.add(prompt)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            delta = {"role": "assistant", "content": "LOCAL ANSWER"}
            if use_tool:
                delta = {"role": "assistant", "tool_calls": [{"index": 0, "id": f"local-call-{len(prompted)}",
                         "type": "function", "function": {"name": tool_call[0],
                         "arguments": json.dumps(tool_call[1])}}]}
            chunks = [{"choices": [{"index": 0, "delta": delta, "finish_reason": None}]},
                      {"choices": [{"index": 0, "delta": {}, "finish_reason": "tool_calls" if use_tool else "stop"}]},
                      {"choices": [], "usage": {"prompt_tokens": 10, "completion_tokens": 10, "total_tokens": 20}}]
            for chunk in chunks:
                chunk.update(id="local", object="chat.completion.chunk", created=1, model="local")
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


class AcpClient:
    def __init__(self, binary, env, cwd):
        self.process = subprocess.Popen([binary], cwd=cwd, env=env, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.frames = queue.Queue()
        self.history = []
        self.tools = {}
        self.counter = 0
        self.permissions = []
        self.responses = []
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        for line in self.process.stdout:
            self.frames.put(json.loads(line))

    def send(self, frame):
        self.process.stdin.write(json.dumps(frame) + "\n")
        self.process.stdin.flush()

    def call(self, method, params, invocation=None):
        self.counter += 1
        call_id = self.counter
        self.send({"jsonrpc": "2.0", "id": call_id, "method": method, "params": params})
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                frame = self.frames.get(timeout=max(.01, deadline - time.monotonic()))
            except queue.Empty:
                raise AssertionError(f"ACP timed out on {method}; frames={self.history}") from None
            self.history.append(frame)
            if frame.get("method") == "session/update":
                update = frame["params"]["update"]
                tool_id = update.get("toolCallId")
                if tool_id:
                    self.tools.setdefault(tool_id, {}).update(update)
            elif frame.get("method") == "session/request_permission":
                params = frame["params"]
                tool = {**self.tools.get(params["toolCall"]["toolCallId"], {}), **params["toolCall"]}
                permissions = []
                for option in params["options"]:
                    for permission in option.get("_meta", {}).get("required_permissions", []):
                        if permission not in permissions:
                            permissions.append(permission)
                action = {"native_method": "session/request_permission",
                          "action": {"tool_name": tool.get("_meta", {}).get("tool_name"),
                                     "input": tool.get("rawInput")},
                          "required_permissions": permissions}
                allow = invocation is not None and grants.automatic_decision(action, invocation)
                kind = "allow_once" if allow else "reject_once"
                option_id = next(option["optionId"] for option in params["options"] if option["kind"] == kind)
                self.permissions.append(action)
                self.responses.append(allow)
                self.send({"jsonrpc": "2.0", "id": frame["id"], "result": {
                    "outcome": {"outcome": "selected", "optionId": option_id}}})
            elif frame.get("id") == call_id and "method" not in frame:
                assert "error" not in frame, frame
                return frame["result"]
        raise AssertionError(f"ACP timed out on {method}")

    def close(self):
        self.process.terminate()
        try:
            self.process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate(timeout=5)
        self.reader.join(timeout=1)


@pytest.mark.parametrize("case", ["exact", "mismatch", "redirect", "denylist", "never", "default-allow", "user-allow"])
def test_vibe_acp_grant_effects(tmp_path, case):
    binary = shutil.which("vibe-acp")
    vibe = shutil.which("vibe")
    if not binary or not vibe:
        pytest.skip("Vibe ACP not installed")
    if subprocess.run([vibe, "--version"], capture_output=True, text=True, timeout=10).stdout.strip() != "vibe 3.0.3":
        pytest.skip("qualified against Vibe 3.0.3")
    home, config, repo = (tmp_path / name for name in ("home", "vibe", "repo"))
    for directory in (home, config, repo):
        directory.mkdir()
    # make writes a marker only for the authorized target; no external tooling.
    (repo / "Makefile").write_text("test:\n\ttouch marker\nextra:\n\ttouch extra-marker\n")
    command = {"mismatch": "make test extra", "redirect": "make test > redirected", "default-allow": "pwd",
               "user-allow": "touch marker"}.get(case, "make test")
    settings = {"denylist": '[tools.bash]\ndenylist=["make"]\n',
                "never": '[tools.bash]\npermission="never"\n',
                "user-allow": '[tools.bash]\nallowlist=["touch"]\n'}.get(case, "")
    env = {key: value for key, value in os.environ.items() if key in ("PATH", "TMPDIR", "LANG", "SYSTEMROOT")}
    env.update(HOME=str(home), VIBE_HOME=str(config), DO_NOT_TRACK="1",
               MISTRAL_API_KEY="local-only-placeholder", HARFLOW_LOCAL_TEST_KEY="local-only-placeholder")
    invocation = grants.configure(Invocation([binary]), ("make test",), "write_in_repo", repo, "3.0.3")
    with provider("denial-shape", ("bash", {"command": command})) as (url, requests):
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
        client = AcpClient(binary, env, repo)
        try:
            client.call("initialize", {"protocolVersion": 1, "clientCapabilities": {},
                                       "clientInfo": {"name": "harflow-local-test", "version": "1"}})
            session = client.call("session/new", {"cwd": str(repo), "mcpServers": []})["sessionId"]
            client.call("session/prompt", {"sessionId": session,
                        "prompt": [{"type": "text", "text": "isolated grant measurement"}]}, invocation)
            assert len(requests) >= 2
            assert (repo / "marker").exists() == (case in ("exact", "user-allow")), client.history
            assert not (repo / "extra-marker").exists()
            assert not (repo / "redirected").exists()
            if case in ("denylist", "never", "default-allow", "user-allow"):
                assert not client.permissions
            else:
                assert client.responses == [case == "exact"], client.permissions
            if case == "exact":
                # The grant is the same frozen dispatch authority on explicit resume.
                client.close()
                (repo / "marker").unlink()
                client = AcpClient(binary, env, repo)
                client.call("initialize", {"protocolVersion": 1, "clientCapabilities": {},
                                           "clientInfo": {"name": "harflow-local-test", "version": "1"}})
                client.call("session/load", {"sessionId": session, "cwd": str(repo), "mcpServers": []})
                requests.clear()
                client.call("session/prompt", {"sessionId": session,
                            "prompt": [{"type": "text", "text": "resumed isolated grant measurement"}]}, invocation)
                assert (repo / "marker").exists(), client.history
                assert client.responses == [True], client.permissions
        finally:
            client.close()
