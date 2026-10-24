"""Shared localhost fake Anthropic API for opt-in Claude CLI certification tests."""

from __future__ import annotations

import http.server
import json
import threading
from collections.abc import Callable
from contextlib import contextmanager

Decision = tuple[list[dict], str]


def _events(blocks: list[dict], stop_reason: str, message_id: str) -> list[tuple[str, dict]]:
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
    return events


@contextmanager
def serve(decide: Callable[[dict], Decision]):
    """Serve `decide(request_body) -> (content_blocks, stop_reason)` as a streamed messages API.

    Yields (base_url, requests), where `requests` collects every request body received."""
    requests: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append(body)
            blocks, stop_reason = decide(body)
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            for name, event in _events(blocks, stop_reason, f"msg_{len(requests)}"):
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


@contextmanager
def fake_anthropic_api(command: str | None = None, *, steps: list[dict] | None = None):
    """A scripted sequence of tool_use steps; `command` is the one-Bash-step shorthand.

    Each step is `{"name", "input", "id"?}`. The step emitted is the number of tool_result blocks
    already in the conversation; once the script is exhausted the model answers with text."""
    script = steps if steps is not None else [{"name": "Bash", "input": {"command": command, "description": "approval fixture"}, "id": "caller_bash_1"}]

    def decide(body: dict) -> Decision:
        answered = sum(
            1
            for message in body.get("messages", [])
            for block in (message.get("content") if isinstance(message.get("content"), list) else [])
            if isinstance(block, dict) and block.get("type") == "tool_result"
        )
        if answered < len(script):
            step = script[answered]
            return [{"type": "tool_use", "id": step.get("id", f"fake_tool_{answered + 1}"), "name": step["name"], "input": step["input"]}], "tool_use"
        return [{"type": "text", "text": "tool rejected"}], "end_turn"

    with serve(decide) as served:
        yield served
