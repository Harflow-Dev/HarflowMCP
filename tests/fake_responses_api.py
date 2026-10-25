"""Localhost fake OpenAI Responses API for Codex CLI tests; no paid model calls.

`LocalResponses` emits one nonce-correlated tool probe per actor. `ScriptedResponses`
emits a caller-supplied sequence of output items (one per model request) so a test can
script shell calls that need escalation, sandboxed calls, apply_patch and a final message.
"""
from contextlib import contextmanager
import http.server
import json
import threading


class LocalResponses:
    """Emit one nonce-correlated tool probe per actor and terminate deterministically."""

    def __init__(self, directory):
        self.directory = directory
        self.requests = []
        self.finished = threading.Event()
        self.nonce = ""
        self.fork = None
        self.commands = []

    @contextmanager
    def serve(self):
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"local fixture")

            def do_POST(self):
                if self.path != "/v1/responses":
                    self.send_error(404)
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                fixture.requests.append(body)
                if len(fixture.requests) > 64:
                    self.send_error(400, "fixture request bound exceeded")
                    return
                inputs = body["input"]
                child = any(item.get("type") == "agent_message" and
                            item.get("recipient", "").startswith("/root/" + fixture.nonce)
                            for item in inputs)
                actor = "child" if child else "parent"
                call_id = fixture.nonce + "_" + actor
                calls = [item for item in inputs if item.get("type") in
                         ("function_call", "custom_tool_call")]

                def function(name, arguments):
                    return dict(id="fc_" + str(len(fixture.requests)), type="function_call",
                                namespace="collaboration", name=name,
                                call_id=call_id + "_" + name + str(len(fixture.requests)),
                                arguments=json.dumps(arguments), status="completed")

                if fixture.fork and not child:
                    spawned = any(item.get("name") == "spawn_agent" and
                                  fixture.nonce in item.get("arguments", "") for item in calls)
                    if not spawned:
                        item = function("spawn_agent", {"task_name": fixture.nonce,
                            "message": fixture.nonce, "fork_turns": fixture.fork})
                    elif not fixture.finished.is_set():
                        item = function("wait_agent", {"timeout_ms": 10000})
                    else:
                        item = None
                elif not any(item.get("call_id") == call_id for item in calls):
                    source = "\n".join(
                        "text({label:" + json.dumps(label) + ",result:await tools.exec_command(" +
                        json.dumps({"cmd": command, "yield_time_ms": 1000}) + ")});"
                        for label, command in fixture.commands)
                    item = dict(id="fc_" + call_id, type="custom_tool_call", namespace="functions",
                                name="exec", call_id=call_id, input=source, status="completed")
                else:
                    if child:
                        fixture.finished.set()
                    item = None
                if item is None:
                    item = dict(id="msg_" + str(len(fixture.requests)), type="message",
                                role="assistant", status="completed", content=[dict(
                                    type="output_text", text=fixture.nonce + " complete", annotations=[])])
                rid = "response_" + str(len(fixture.requests))
                response = dict(id=rid, object="response", status="completed", output=[item],
                                usage=dict(input_tokens=1, output_tokens=1, total_tokens=2))
                events = [dict(type="response.created", response=dict(response, status="in_progress", output=[])),
                          dict(type="response.output_item.added", output_index=0, item=dict(item, status="in_progress")),
                          dict(type="response.output_item.done", output_index=0, item=item),
                          dict(type="response.completed", response=response)]
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for event in events:
                    self.wfile.write(("data: " + json.dumps(event) + "\n\n").encode())
                    self.wfile.flush()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.url = f"http://127.0.0.1:{server.server_port}"
        try:
            yield self
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)
            diagnostic = json.dumps(self.requests)
            if len(diagnostic) > 2_000_000:
                diagnostic = json.dumps({"truncated": True, "tail": diagnostic[-1_000_000:]})
            (self.directory / "requests.json").write_text(diagnostic)

    def results(self):
        results = {}
        for body in self.requests:
            for item in body["input"]:
                if item.get("type") != "custom_tool_call_output" or not item.get("call_id", "").startswith(self.nonce):
                    continue
                for block in item.get("output", []):
                    try:
                        value = json.loads(block.get("text", ""))
                    except (ValueError, AttributeError):
                        continue
                    if "label" in value:
                        results[value["label"]] = value["result"]
        return results


def shell_call(command, *, call_id, escalate=False, justification=None, workdir=None,
               name="shell_command", prefix_rule=None, extra=None):
    """A function_call item for Codex's shell tools.

    `escalate` sets `sandbox_permissions: "require_escalated"` as the tool schema expects;
    the shape is pinned by the recorded request tool list, not assumed.
    """
    arguments = {"command": command}
    if workdir is not None:
        arguments["workdir"] = workdir
    if escalate:
        arguments["sandbox_permissions"] = "require_escalated"
        arguments["justification"] = justification or "needs to run outside the sandbox"
        if prefix_rule is not None:
            arguments["prefix_rule"] = prefix_rule
    arguments.update(extra or {})
    return dict(type="function_call", name=name, call_id=call_id,
                arguments=json.dumps(arguments), status="completed")


def apply_patch_call(patch, *, call_id):
    """A custom_tool_call item for apply_patch (freeform grammar tool)."""
    return dict(type="custom_tool_call", name="apply_patch", call_id=call_id, input=patch,
                status="completed")


def http_error(status=500, message="scripted upstream failure"):
    """Script entry: answer the model request with an HTTP error instead of a stream."""
    return dict(_http_error=status, _message=message)


def response_failed(message="scripted response.failed", code="server_error"):
    """Script entry: answer with a `response.failed` event so the turn fails."""
    return dict(_response_failed=message, _code=code)


def final_message(text):
    return dict(type="message", role="assistant", status="completed",
                content=[dict(type="output_text", text=text, annotations=[])])


class ScriptedResponses:
    """Emit scripted output items, one per model request, then a final assistant message.

    `script` entries are output-item dicts (see `shell_call`, `apply_patch_call`,
    `final_message`) or callables `(body, request_number) -> item`. `requests` records every
    request body. Requests beyond the script get a plain final message so a turn always ends.
    """

    def __init__(self, script=(), directory=None, final_text="scripted complete"):
        self.script = list(script)
        self.directory = directory
        self.final_text = final_text
        self.requests = []
        self.paths = []
        self.url = ""
        self.lock = threading.Lock()

    def _next(self, body, number):
        index = number - 1
        entry = self.script[index] if index < len(self.script) else final_message(self.final_text)
        item = entry(body, number) if callable(entry) else dict(entry)
        if "type" in item:
            item.setdefault("id", ("msg_" if item["type"] == "message" else "fc_") + str(number))
        return item

    @contextmanager
    def serve(self):
        fixture = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"local fixture")

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                with fixture.lock:
                    fixture.paths.append(self.path)
                    fixture.requests.append(body)
                    number = len(fixture.requests)
                if not self.path.endswith("/responses") or number > 64:
                    self.send_error(404 if number <= 64 else 400)
                    return
                item = fixture._next(body, number)
                if "_http_error" in item:
                    self.send_response(item["_http_error"])
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": {"message": item["_message"], "type": "server_error"}}).encode())
                    return
                if "_response_failed" in item:
                    failed = dict(id="response_" + str(number), object="response", status="failed", output=[],
                                  error=dict(code=item["_code"], message=item["_response_failed"]))
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.end_headers()
                    for event in (dict(type="response.created", response=dict(failed, status="in_progress")),
                                  dict(type="response.failed", response=failed)):
                        self.wfile.write(("data: " + json.dumps(event) + "\n\n").encode())
                        self.wfile.flush()
                    return
                response = dict(id="response_" + str(number), object="response", status="completed",
                                output=[item], usage=dict(input_tokens=1, output_tokens=1, total_tokens=2))
                events = [dict(type="response.created", response=dict(response, status="in_progress", output=[])),
                          dict(type="response.output_item.added", output_index=0, item=dict(item, status="in_progress")),
                          dict(type="response.output_item.done", output_index=0, item=item),
                          dict(type="response.completed", response=response)]
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for event in events:
                    self.wfile.write(("data: " + json.dumps(event) + "\n\n").encode())
                    self.wfile.flush()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.url = f"http://127.0.0.1:{server.server_port}"
        try:
            yield self
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)
            if self.directory is not None:
                diagnostic = json.dumps(self.requests)
                if len(diagnostic) > 2_000_000:
                    diagnostic = json.dumps({"truncated": True, "tail": diagnostic[-1_000_000:]})
                (self.directory / "requests.json").write_text(diagnostic)
