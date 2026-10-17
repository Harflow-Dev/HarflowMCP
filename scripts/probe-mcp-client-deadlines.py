#!/usr/bin/env python3
"""Measure how long MCP clients let a tool call run, at no model cost.

A localhost fake model API (Anthropic /v1/messages and OpenAI /v1/responses, both SSE) answers
the first client request with a call to a stub MCP tool, `hold`, that sleeps for N seconds and
optionally reports progress every 5 s. The script records when the tool started, whether the
client sent a progressToken, whether the result reached the "model" and after how long, and
whether the server saw a cancellation. The stub server is this same file run with `--stub`.

Usage (run with the repo venv so `mcp` is importable):

    uv run python scripts/probe-mcp-client-deadlines.py claude 70 --progress --name claude-70p
    uv run python scripts/probe-mcp-client-deadlines.py codex 130 --name codex-130 \
        --codex-tool-timeout 60
    uv run python scripts/probe-mcp-client-deadlines.py claude 60 --name c --env MCP_TOOL_TIMEOUT=30000

Each run uses an isolated HOME / CLAUDE_CONFIG_DIR / CODEX_HOME under --work-dir/<name>
(default: a fresh temp directory) and prints a JSON summary, also saved as summary.json there.
Codex `exec` drives the tool through its `exec` code cell and polls it with `wait`.
This is a manual measurement tool, not a pytest test; it needs the `claude` / `codex` binaries.
"""
import argparse, asyncio, http.server, json, os, re, shutil, subprocess, sys, tempfile, threading, time
from pathlib import Path


def stub_main():
    from mcp.server.mcpserver import MCPServer as FastMCP, Context

    LOG = os.environ["STUB_LOG"]
    mcp = FastMCP("stub")

    def note(**kw):
        with open(LOG, "a") as f:
            f.write(json.dumps({"t": time.time(), **kw}) + "\n")

    @mcp.tool()
    async def hold(seconds: int, progress: bool, ctx: Context) -> str:
        """Hold this request open for `seconds`, optionally reporting progress every 5 s."""
        rc = ctx.request_context
        meta = getattr(rc, "meta", None)
        token = None
        for name in ("progress_token", "progressToken"):
            token = token or getattr(meta, name, None) or (meta.get(name) if isinstance(meta, dict) else None) or getattr(rc, name, None)
        note(event="start", seconds=seconds, progress=progress, progress_token=token, meta=repr(meta)[:300])
        start = time.monotonic()
        step = 0
        try:
            while time.monotonic() - start < seconds:
                await asyncio.sleep(min(5, seconds - (time.monotonic() - start)))
                if progress and time.monotonic() - start < seconds:
                    step += 1
                    await ctx.report_progress(step, total=None, message=f"held {time.monotonic()-start:.0f}s")
                    note(event="progress", step=step, sent=token is not None)
        except asyncio.CancelledError:
            note(event="cancelled", elapsed=time.monotonic() - start)
            raise
        note(event="finish", elapsed=time.monotonic() - start)
        return f"HELD_OK {seconds}"

    note(event="server_up", argv=sys.argv)
    try:
        mcp.run()
    finally:
        note(event="server_down")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("client", choices=["claude", "codex"])
    ap.add_argument("seconds", type=int)
    ap.add_argument("--progress", action="store_true")
    ap.add_argument("--codex-tool-timeout", type=float)
    ap.add_argument("--env", action="append", default=[])
    ap.add_argument("--name", required=True)
    ap.add_argument("--work-dir", type=Path, help="parent directory for run output (default: a fresh temp dir)")
    a = ap.parse_args()

    here = Path(__file__).resolve()
    if not a.name or Path(a.name).name != a.name or a.name in (".", "..") or "\\" in a.name:
        ap.error("--name must be a single path component (no separators, '..' or absolute paths)")
    root = a.work_dir or Path(tempfile.mkdtemp(prefix="pb-probe-"))
    work = root / a.name
    if work.exists():
        ap.error(f"{work} already exists; choose another --name or --work-dir")
    (work / "repo").mkdir(parents=True)
    subprocess.run(["git", "init", "-q"], cwd=work / "repo")
    stub_log = work / "stub.jsonl"
    api_log = work / "api.jsonl"
    py = sys.executable
    state = {"tool_name": None}

    def api_note(**kw):
        with api_log.open("a") as f:
            f.write(json.dumps({"t": time.time(), **kw}) + "\n")

    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *x): pass
        def do_GET(self):
            self.send_response(404); self.end_headers()
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            path = self.path.split("?")[0]
            if path.endswith("count_tokens"):
                self.send_response(200); self.end_headers(); self.wfile.write(b'{"input_tokens":10}'); return
            if path.endswith("/messages"):
                self.anthropic(body)
            elif path.endswith("/responses"):
                self.openai_responses(body)
            else:
                api_note(event="unknown_path", path=self.path); self.send_response(404); self.end_headers()

        def anthropic(self, body):
            tools = [t.get("name") for t in body.get("tools", [])]
            name = next((n for n in tools if n and n.endswith("hold")), None)
            results = [b for m in body["messages"] for b in m.get("content", []) if isinstance(b, dict) and b.get("type") == "tool_result"]
            if name is None and not results:
                # side requests (titles etc.)
                blocks = [{"type": "text", "text": "ok"}]
            elif not results:
                api_note(event="call_tool", name=name)
                blocks = [{"type": "tool_use", "id": "toolu_hold", "name": name, "input": {"seconds": a.seconds, "progress": a.progress}}]
            else:
                api_note(event="tool_result", content=json.dumps(results[-1])[:2000])
                blocks = [{"type": "text", "text": "DONE"}]
            ev = [("message_start", {"type": "message_start", "message": {"id": "m", "type": "message", "role": "assistant", "content": [], "model": "claude-sonnet-4-6", "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": 10, "output_tokens": 0}}})]
            for i, b in enumerate(blocks):
                init = dict(b)
                if b["type"] == "text":
                    init["text"] = ""; d = {"type": "text_delta", "text": b["text"]}
                else:
                    init["input"] = {}; d = {"type": "input_json_delta", "partial_json": json.dumps(b["input"])}
                ev += [("content_block_start", {"type": "content_block_start", "index": i, "content_block": init}),
                       ("content_block_delta", {"type": "content_block_delta", "index": i, "delta": d}),
                       ("content_block_stop", {"type": "content_block_stop", "index": i})]
            ev += [("message_delta", {"type": "message_delta", "delta": {"stop_reason": "tool_use" if blocks[0]["type"] == "tool_use" else "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 5}}),
                   ("message_stop", {"type": "message_stop"})]
            self.send_response(200); self.send_header("Content-Type", "text/event-stream"); self.end_headers()
            for n, e in ev:
                self.wfile.write(f"event: {n}\ndata: {json.dumps(e)}\n\n".encode()); self.wfile.flush()

        def openai_responses(self, body):
            if state["tool_name"] is None:
                (work / "codex-request.json").write_text(json.dumps(body, indent=1)[:400000])
            inputs = body["input"]
            outs = [i for i in inputs if i.get("type") in ("function_call_output", "custom_tool_call_output", "mcp_tool_call_output")]
            def find(tools):
                for t in tools or []:
                    if t.get("name", "").endswith("hold"):
                        return t, None
                    if t.get("type") == "namespace":
                        sub, _ = find(t.get("tools"))
                        if sub: return sub, t.get("name")
                return None, None
            if not outs:
                api_note(event="call_tool", name="exec:mcp__stub__hold")
                code = 'text(JSON.stringify(await tools.mcp__stub__hold({seconds: %d, progress: %s})));' % (a.seconds, "true" if a.progress else "false")
                item = {"id": "ct_hold", "type": "custom_tool_call", "namespace": "functions", "name": "exec", "call_id": "call_hold", "status": "completed", "input": code}
            else:
                last = json.dumps(outs[-1])
                m = re.search(r"Script running with cell ID (\\w+)", last.replace("\\\\", "\\"))
                m = m or re.search(r"Script running with cell ID ([A-Za-z0-9_-]+)", last)
                if m:
                    api_note(event="yield", cell=m.group(1), n=len(outs))
                    item = {"id": "fc_wait%d" % len(outs), "type": "function_call", "namespace": "functions", "name": "wait", "call_id": "call_wait%d" % len(outs), "arguments": json.dumps({"cell_id": m.group(1), "yield_time_ms": 30000}), "status": "completed"}
                else:
                    api_note(event="tool_result", content=last[:2000])
                    item = {"id": "msg", "type": "message", "role": "assistant", "content": [{"type": "output_text", "text": "DONE", "annotations": []}], "status": "completed"}
            ev = [{"type": "response.created", "response": {"id": "r", "object": "response", "status": "in_progress", "output": []}},
                  {"type": "response.output_item.added", "output_index": 0, "item": dict(item, status="in_progress")},
                  {"type": "response.output_item.done", "output_index": 0, "item": item},
                  {"type": "response.completed", "response": {"id": "r", "object": "response", "status": "completed", "output": [item], "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}}}]
            self.send_response(200); self.send_header("Content-Type", "text/event-stream"); self.end_headers()
            for e in ev:
                self.wfile.write(("data: " + json.dumps(e) + "\n\n").encode()); self.wfile.flush()

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{srv.server_port}"
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ANTHROPIC_", "CLAUDE_", "OPENAI_", "CODEX_")) and k != "CLAUDECODE"}
    env["STUB_LOG"] = str(stub_log)
    for kv in a.env:
        k, v = kv.split("=", 1); env[k] = v
    stub_cmd = [py, str(here), "--stub"]
    if a.client == "claude":
        cfg = work / "mcp.json"
        cfg.write_text(json.dumps({"mcpServers": {"stub": {"command": stub_cmd[0], "args": stub_cmd[1:], "env": {"STUB_LOG": str(stub_log)}}}}))
        env.update(HOME=str(work / "home"), CLAUDE_CONFIG_DIR=str(work / "config"), ANTHROPIC_API_KEY="fake-local-key", ANTHROPIC_BASE_URL=url, CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC="1")
        argv = ["claude", "-p", "--output-format", "stream-json", "--verbose", "--strict-mcp-config", "--mcp-config", str(cfg),
                "--allowedTools", "mcp__stub__hold", "--model", "claude-sonnet-4-6", "--", "call hold"]
    else:
        home = work / "codex-home"; home.mkdir()
        extra = f"tool_timeout_sec = {a.codex_tool_timeout}\n" if a.codex_tool_timeout else ""
        (home / "config.toml").write_text(
            'model_provider = "local"\nmodel = "gpt-6.1-sol"\n'
            f'[model_providers.local]\nname = "local"\nbase_url = "{url}/v1"\nwire_api = "responses"\nrequires_openai_auth = false\n'
            f'[mcp_servers.stub]\ncommand = "{stub_cmd[0]}"\nargs = ["{stub_cmd[1]}", "{stub_cmd[2]}"]\ndefault_tools_approval_mode = "approve"\n{extra}'
            f'[mcp_servers.stub.env]\nSTUB_LOG = "{stub_log}"\n')
        env["CODEX_HOME"] = str(home)
        argv = ["codex", "exec", "--json", "--skip-git-repo-check", "-s", "read-only", "-c", 'approval_policy="never"', "--", "call hold"]
    t0 = time.time()
    api_note(event="client_start", argv=argv)
    p = subprocess.run(argv, cwd=work / "repo", env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=a.seconds + 900)
    api_note(event="client_exit", code=p.returncode, elapsed=time.time() - t0)
    (work / "stdout.txt").write_text(p.stdout); (work / "stderr.txt").write_text(p.stderr)
    srv.shutdown()
    stub = [json.loads(l) for l in stub_log.read_text().splitlines()] if stub_log.exists() else []
    api = [json.loads(l) for l in api_log.read_text().splitlines()]
    start = next((e["t"] for e in stub if e["event"] == "start"), None)
    res = next((e for e in api if e["event"] == "tool_result"), None)
    summary = {"name": a.name, "client": a.client, "hold": a.seconds, "progress": a.progress,
               "tool_started": start is not None,
               "progress_token": next((e.get("progress_token") for e in stub if e["event"] == "start"), None),
               "progress_sent": sum(1 for e in stub if e["event"] == "progress"),
               "stub_end": [ (e["event"], round(e["t"] - start, 1)) for e in stub if e["event"] in ("finish", "cancelled", "server_down")] if start else None,
               "result_after_s": round(res["t"] - start, 1) if res and start else None,
               "result": res["content"][:400] if res else None,
               "exit": p.returncode, "client_elapsed": round(time.time() - t0, 1)}
    (work / "summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary))


if __name__ == "__main__":
    if "--stub" in sys.argv[1:2]:
        stub_main()
    else:
        main()
