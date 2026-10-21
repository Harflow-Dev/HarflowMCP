"""Regression coverage for the sequential release stdio handshake."""

import anyio
import importlib.util
import json
import os
from pathlib import Path
import sys
import time


ROOT = Path(__file__).parents[1]
SPEC = importlib.util.spec_from_file_location("release_stdio", ROOT / "scripts" / "release_stdio.py")
release_stdio = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(release_stdio)


def _fake_server(path, stalled=False, server_name="harflow", missing_tools=False):
    names = sorted(release_stdio.REQUIRED_TOOLS - ({"start_workflow"} if missing_tools else set()))
    tools = [{"name": name, "description": "fixture", "inputSchema": {"type": "object"}} for name in names]
    info = json.dumps({"jsonrpc": "2.0", "id": "REQUEST_ID", "result": {"protocolVersion": "2025-11-25", "capabilities": {}, "serverInfo": {"name": server_name, "version": ""}}})
    listing = json.dumps({"jsonrpc": "2.0", "id": "REQUEST_ID", "result": {"tools": tools}})
    source = f'''import json, os, select, sys, time
PID_FILE = sys.argv[1]
open(PID_FILE, "w").write(str(os.getpid()))
for line in sys.stdin:
    request = json.loads(line)
    if request.get("method") == "initialize":
        response = json.loads({info!r})
        response["id"] = request["id"]
        print(json.dumps(response), flush=True)
    elif request.get("method") == "tools/list":
        deadline = time.monotonic() + ({"30" if stalled else "0.15"})
        while time.monotonic() < deadline:
            if select.select([sys.stdin], [], [], 0)[0] and not os.read(sys.stdin.fileno(), 1):
                print("premature stdin close", file=sys.stderr, flush=True)
                raise SystemExit(3)
            time.sleep(0.01)
        response = json.loads({listing!r})
        response["id"] = request["id"]
        print(json.dumps(response), flush=True)
'''
    path.write_text(source)


def _has_leaf(error, kind, text=None):
    if isinstance(error, kind) and (text is None or text in str(error)):
        return True
    return any(_has_leaf(child, kind, text) for child in getattr(error, "exceptions", ()))


def test_delayed_tools_response_keeps_stdin_open_through_list_tools(tmp_path):
    server = tmp_path / "delayed_server.py"
    _fake_server(server)
    pid_file = tmp_path / "delayed.pid"

    async def run():
        await release_stdio.check_stdio([sys.executable, str(server), str(pid_file)], dict(), timeout=3)

    anyio.run(run)


def test_stalled_tools_response_is_bounded_by_helper_timeout(tmp_path):
    server = tmp_path / "stalled_server.py"
    _fake_server(server, stalled=True)
    pid_file = tmp_path / "stalled.pid"

    async def run():
        with anyio.fail_after(3):
            try:
                await release_stdio.check_stdio([sys.executable, str(server), str(pid_file)], dict(), timeout=0.2)
            except BaseException as error:
                assert _has_leaf(error, TimeoutError)
                deadline = time.monotonic() + 2
                while time.monotonic() < deadline:
                    if pid_file.exists():
                        try:
                            os.kill(int(pid_file.read_text()), 0)
                        except ProcessLookupError:
                            return
                    await anyio.sleep(0.01)
                raise AssertionError("stdio server survived helper cleanup")
        raise AssertionError("stdio helper did not enforce its timeout")

    anyio.run(run)


def test_wrong_server_name_is_rejected(tmp_path):
    server = tmp_path / "wrong_server.py"
    _fake_server(server, server_name="other")

    async def run():
        try:
            await release_stdio.check_stdio([sys.executable, str(server), str(tmp_path / "wrong.pid")], dict(), timeout=3)
        except BaseException as error:
            assert _has_leaf(error, ValueError, "identity")
            return
        raise AssertionError("unexpected server identity accepted")

    anyio.run(run)


def test_missing_core_tool_is_rejected(tmp_path):
    server = tmp_path / "incomplete_server.py"
    _fake_server(server, missing_tools=True)

    async def run():
        try:
            await release_stdio.check_stdio([sys.executable, str(server), str(tmp_path / "incomplete.pid")], dict(), timeout=3)
        except BaseException as error:
            assert _has_leaf(error, ValueError, "required release tools")
            return
        raise AssertionError("incomplete tool set accepted")

    anyio.run(run)
