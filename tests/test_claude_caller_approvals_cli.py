"""Opt-in Claude caller-approval certification against a localhost fake Anthropic API."""

from __future__ import annotations

import asyncio
import http.server
import json
import os
import shutil
import subprocess
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

from polybridge import approval_runtime, approvals
from polybridge.backends import get
from polybridge.tasks import TaskRegistry


pytestmark = [
    pytest.mark.cli_integration,
    pytest.mark.skipif(not os.environ.get("PB_CLI_INTEGRATION"), reason="opt in with PB_CLI_INTEGRATION=1"),
]


@contextmanager
def fake_anthropic_api(command: str):
    requests: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            saw_result = any(
                block.get("type") == "tool_result"
                for message in body.get("messages", [])
                for block in (message.get("content") or [])
                if isinstance(block, dict)
            )
            blocks = ([{"type": "text", "text": "tool rejected"}] if saw_result else [{"type": "tool_use", "id": "caller_bash_1", "name": "Bash", "input": {"command": command, "description": "approval fixture"}}])
            stop_reason = "end_turn" if saw_result else "tool_use"
            events = [
                ("message_start", {"type": "message_start", "message": {"id": f"msg_{len(requests)}", "type": "message", "role": "assistant", "content": [], "model": "claude-sonnet-4-6", "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 0}}}),
            ]
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
                ("message_delta", {"type": "message_delta", "delta": {"stop_reason": stop_reason, "stop_sequence": None}, "usage": {"output_tokens": 1}}),
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


async def _wait_for_request(registry: TaskRegistry, task_id: str) -> dict:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        rows = approvals.rows(registry.log_dir, task_id)
        if rows:
            return rows[0]
        await asyncio.sleep(0.05)
    raise AssertionError("Claude did not emit a caller approval request")


@pytest.mark.parametrize("decision,marker_expected,outcome", [("approve", True, "succeeded"), ("reject", False, "failed")])
@pytest.mark.asyncio
async def test_real_claude_taskregistry_caller_approval_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, decision: str, marker_expected: bool, outcome: str) -> None:
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if not version.startswith("2.1.302"):
        pytest.skip(f"Caller approval certification is pinned to Claude 2.1.302; found {version}")
    repo = tmp_path / "repo"
    repo.mkdir()
    marker = repo / "approval-marker.txt"
    command = "python3 -c \"open('approval-marker.txt','a').write('APPROVED')\""
    with fake_anthropic_api(command) as (base_url, requests):
        for key in tuple(os.environ):
            if key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) or key == "CLAUDECODE":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "config"))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "local-fake-key")
        monkeypatch.setenv("ANTHROPIC_BASE_URL", base_url)
        monkeypatch.setenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        registry = TaskRegistry(log_dir=tmp_path / "logs", open_monitor=False)
        task = await registry.start("run one exact Bash action", repo, backend=get("claude"), freedom="write_in_repo", caller_approvals=True)
        try:
            row = await _wait_for_request(registry, task.task_id)
            assert task.proc is not None and task.proc.returncode is None
            assert not marker.exists()
            full = await approval_runtime.get_request(registry, task.task_id, row["request_id"])
            assert full["action"]["input"]["command"] == command
            answered = await approval_runtime.respond_request(registry, task.task_id, row["request_id"], full["action_digest"], full["owner_revision"], decision)
            assert answered["decision"] == decision
            await asyncio.wait_for(task.done.wait(), timeout=20)
            final = next(item for item in approvals.rows(registry.log_dir, task.task_id) if item["request_id"] == row["request_id"])
            assert marker.exists() is marker_expected
            assert final["execution_outcome"] == outcome
            assert final["action"]["input"]["command"] == command
            assert requests
        finally:
            if not task.done.is_set():
                await registry.cancel(task.task_id)

