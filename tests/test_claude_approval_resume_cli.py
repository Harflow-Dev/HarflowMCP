"""Opt-in persisted-session resume coverage for Claude caller approvals."""

from __future__ import annotations

import asyncio
import http.server
import json
import os
import shutil
import subprocess
import threading
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
def _resume_api(first_command: str, second_command: str):
    requests: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            texts = [
                block.get("text", "")
                for message in body.get("messages", [])
                if message.get("role") == "user"
                for block in (message.get("content") if isinstance(message.get("content"), list) else [{"text": message.get("content", "")}])
                if isinstance(block, dict)
            ]
            resumed = any("resume approval action" in text for text in texts)
            command = second_command if resumed else first_command
            tool_id = "resume_bash_1" if resumed else "initial_bash_1"
            # A tool result after the latest user prompt means Claude is ready to finish.
            latest_user = max((index for index, message in enumerate(body.get("messages", [])) if message.get("role") == "user"), default=-1)
            has_current_result = any(
                index > latest_user and message.get("role") == "user"
                and any(isinstance(block, dict) and block.get("type") == "tool_result" for block in (message.get("content") or []))
                for index, message in enumerate(body.get("messages", []))
            )
            if has_current_result:
                blocks = [{"type": "text", "text": "completed"}]
                stop_reason = "end_turn"
            else:
                blocks = [{"type": "tool_use", "id": tool_id, "name": "Bash", "input": {"command": command, "description": "resume approval fixture"}}]
                stop_reason = "tool_use"
            message_id = f"msg_{len(requests)}"
            events = [("message_start", {"type": "message_start", "message": {"id": message_id, "type": "message", "role": "assistant", "content": [], "model": "claude-sonnet-4-6", "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 1, "output_tokens": 0}}})]
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

    api = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=api.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{api.server_port}", requests
    finally:
        api.shutdown()
        api.server_close()
        thread.join(timeout=2)


async def _wait_for_request(registry: TaskRegistry, task_id: str, tool_id: str) -> dict:
    for _ in range(400):
        rows = approvals.rows(registry.log_dir, task_id)
        match = next((row for row in rows if row["tool_use_id"] == tool_id), None)
        if match:
            return match
        await asyncio.sleep(0.05)
    raise AssertionError(f"Claude did not emit {tool_id}; rows={rows!r}")


@pytest.mark.asyncio
async def test_claude_caller_approvals_survive_persisted_session_resume(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    executable = shutil.which("claude")
    if not executable:
        pytest.skip("Claude CLI not installed")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True, check=True).stdout.strip()
    if not version.startswith("2.1.302"):
        pytest.skip(f"Caller approval certification is pinned to Claude 2.1.302; found {version}")
    repo = tmp_path / "repo"
    repo.mkdir()
    first = "python3 -c \"open('first-marker.txt','a').write('APPROVED')\""
    second = "python3 -c \"open('second-marker.txt','a').write('APPROVED')\""
    with _resume_api(first, second) as (base_url, requests):
        for key in tuple(os.environ):
            if key.startswith(("ANTHROPIC_", "CLAUDE_", "AWS_", "GOOGLE_")) or key == "CLAUDECODE":
                monkeypatch.delenv(key, raising=False)
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "config"))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "local-fake-key")
        monkeypatch.setenv("ANTHROPIC_BASE_URL", base_url)
        monkeypatch.setenv("CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC", "1")
        registry = TaskRegistry(log_dir=tmp_path / "logs", open_monitor=False)
        task = await registry.start("run initial approval action", repo, backend=get("claude"), freedom="write_in_repo", caller_approvals=True)
        resumed = None
        try:
            initial = await _wait_for_request(registry, task.task_id, "initial_bash_1")
            assert not (repo / "first-marker.txt").exists()
            full = await approval_runtime.get_request(registry, task.task_id, initial["request_id"])
            await approval_runtime.respond_request(registry, task.task_id, initial["request_id"], full["action_digest"], full["owner_revision"], "approve")
            await asyncio.wait_for(task.done.wait(), timeout=20)
            resumed = await registry.resume(task, "resume approval action")
            assert resumed.caller_approvals is True
            assert resumed.session_id == task.session_id
            followup = await _wait_for_request(registry, resumed.task_id, "resume_bash_1")
            assert not (repo / "second-marker.txt").exists()
            resumed_full = await approval_runtime.get_request(registry, resumed.task_id, followup["request_id"])
            await approval_runtime.respond_request(registry, resumed.task_id, followup["request_id"], resumed_full["action_digest"], resumed_full["owner_revision"], "approve")
            await asyncio.wait_for(resumed.done.wait(), timeout=20)
            assert (repo / "first-marker.txt").read_text() == "APPROVED"
            assert (repo / "second-marker.txt").read_text() == "APPROVED"
            assert approvals.rows(registry.log_dir, resumed.task_id)[0]["execution_outcome"] == "succeeded"
            assert len(requests) >= 4
        finally:
            if resumed is not None and not resumed.done.is_set():
                await registry.cancel(resumed)
            if not task.done.is_set():
                await registry.cancel(task)
