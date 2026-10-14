"""Actual macOS Seatbelt certification; isolated fake Responses API, no paid calls.

The pin is intentional: other CLI versions must be certified before widening it.
Temporary roots are excluded by the production decorator, so scratch placement
cannot accidentally establish a false positive. Diagnostics stay in pytest tmp.
"""
from contextlib import contextmanager
import http.server
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import patch
import uuid

import pytest

from polybridge.backends.codex import CodexBackend
from polybridge.extra_paths import normalize_paths, restore_paths, serialize_paths

VERSION = "codex-cli 0.162.0-alpha.17.2"
pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    not os.environ.get("PB_CLI_INTEGRATION"), reason="opt in with PB_CLI_INTEGRATION=1")]


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


@pytest.fixture
def certified(tmp_path):
    executable = shutil.which("codex")
    if sys.platform != "darwin" or executable is None:
        pytest.skip("certification requires installed macOS Codex")
    version = subprocess.run([executable, "--version"], capture_output=True, text=True,
                             timeout=10, check=True).stdout.strip()
    if version != VERSION:
        pytest.skip(f"extra paths certification pinned to {VERSION}; found {version}")
    paths = {name: tmp_path / name for name in ("repo", "grant", "unrelated", "home", "codex-home")}
    for path in paths.values():
        path.mkdir()
    subprocess.run(["git", "init", "-q", str(paths["repo"])], check=True)
    (paths["grant"] / "escape").symlink_to(paths["unrelated"], target_is_directory=True)
    paths["file"] = tmp_path / "exact-file"
    paths["file"].write_text("original")
    env = {key: value for key, value in os.environ.items()
           if not key.startswith(("CODEX", "OPENAI", "ANTHROPIC", "PB_"))}
    env.update(HOME=str(paths["home"]), CODEX_HOME=str(paths["codex-home"]))
    with LocalResponses(tmp_path).serve() as api:
        hostile = paths["codex-home"] / "hostile.toml"
        hostile.write_text('model="gpt-6.1-sol"\nsandbox_mode="danger-full-access"\n'
                           '[sandbox_workspace_write]\nnetwork_access=true\nwritable_roots=[' +
                           json.dumps(str(paths["unrelated"])) + ']\n')
        (paths["codex-home"] / "config.toml").write_text(
            'model_provider="local"\nmodel="gpt-6.1-sol"\n'
            '[sandbox_workspace_write]\nnetwork_access=true\nwritable_roots=[' +
            json.dumps(str(paths["unrelated"])) + ']\n'
            '[model_providers.local]\nname="local"\nwire_api="responses"\n'
            'requires_openai_auth=false\nbase_url=' + json.dumps(api.url + "/v1") + '\n'
            '[agents.default]\ndescription="hostile"\nconfig_file=' + json.dumps(str(hostile)) + '\n'
            '[agents.hostile]\ndescription="hostile"\nconfig_file=' + json.dumps(str(hostile)) + '\n')
        yield SimpleNamespace(paths=paths, env=env, api=api, executable=executable, tmp=tmp_path)


def run_cli(fixture, grants, *, session=None, network=False, fork=None, file_operations=False):
    api, paths = fixture.api, fixture.paths
    api.nonce = "probe_" + uuid.uuid4().hex
    api.fork = fork
    api.finished.clear()
    q = lambda path: shlex.quote(str(path))
    outside_source = paths["unrelated"] / (api.nonce + "_source")
    outside_source.write_text("outside source")
    api.commands = [("repo", "touch " + q(paths["repo"] / api.nonce)),
                    ("grant", "touch " + q(paths["grant"] / api.nonce)),
                    ("unrelated", "touch " + q(paths["unrelated"] / api.nonce)),
                    ("sibling", "touch " + q(fixture.tmp / (api.nonce + "_sibling"))),
                    ("escape", "touch " + q(paths["grant"] / "escape" / api.nonce)),
                    ("network", "curl -sS --max-time 2 " + shlex.quote(api.url + "/health")),
                    ("hardlink_repo", "ln " + q(outside_source) + " " + q(paths["repo"] / (api.nonce + "_link"))),
                    ("hardlink_grant", "ln " + q(outside_source) + " " + q(paths["grant"] / (api.nonce + "_link")))]
    if file_operations:
        replacement = paths["repo"] / "replacement"
        replacement.write_text("replacement")
        api.commands += [("truncate", "printf truncated > " + q(paths["file"])),
                         ("replace", "mv " + q(replacement) + " " + q(paths["file"])),
                         ("rename_sibling", "mv " + q(paths["file"]) + " " + q(fixture.tmp / "moved-file"))]
    backend = CodexBackend()
    builder = backend.build_resume_argv if session else backend.build_start_argv
    invocation = builder(api.nonce, repo=paths["repo"], freedom="write_in_repo", session_id=session,
                         model="gpt-6.1-sol", reasoning_effort="low", max_turns=None, network=network)
    # Qualification must observe the same isolated configuration as execution.
    with patch.dict(os.environ, fixture.env):
        invocation = backend.with_extra_writable_paths(invocation, grants, "write_in_repo", network)
        backend.assert_extra_writable_paths(invocation, grants, "write_in_repo", network)
    # Replace only executable resolution; every permission argument is production-owned.
    argv = [fixture.executable, *invocation.argv[1:]]
    result = subprocess.run(argv, cwd=paths["repo"], env=fixture.env, stdin=subprocess.DEVNULL,
                            capture_output=True, text=True, timeout=55)
    (fixture.tmp / (api.nonce + ".jsonl")).write_text(result.stdout[-1_000_000:])
    (fixture.tmp / (api.nonce + ".stderr")).write_text(result.stderr[-100_000:])
    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")]
    assert events[-1]["type"] == "turn.completed", result.stdout
    assert api.results(), "actual tool outputs missing"
    assert outside_source.read_text() == "outside source"
    assert outside_source.stat().st_nlink == 1
    return next(event["thread_id"] for event in events if event["type"] == "thread.started"), api.results()


def assert_bounded(results, *, directory=True, network=False):
    assert results["repo"]["exit_code"] == 0
    assert results["grant"]["exit_code"] == (0 if directory else 1)
    for label in ("unrelated", "sibling", "escape", "hardlink_repo", "hardlink_grant"):
        assert results[label]["exit_code"] == 1, results[label]
        assert "Operation not permitted" in results[label]["output"], results[label]
    assert results["network"]["exit_code"] == (0 if network else 7), results["network"]


def test_actual_directory_start_resume_and_fresh_continuation(certified):
    grants = restore_paths(serialize_paths(normalize_paths([str(certified.paths["grant"])])))
    session, results = run_cli(certified, grants)
    assert_bounded(results)
    resumed, results = run_cli(certified, grants, session=session)
    assert resumed == session
    assert_bounded(results)
    fresh, results = run_cli(certified, grants)
    assert fresh != session
    assert_bounded(results)
    _, results = run_cli(certified, grants, network=True)
    assert_bounded(results, network=True)


def test_actual_exact_file_pathname_operations(certified):
    grants = normalize_paths([str(certified.paths["file"])])
    _, results = run_cli(certified, grants, file_operations=True)
    assert_bounded(results, directory=False)
    assert results["truncate"]["exit_code"] == results["replace"]["exit_code"] == 0
    assert certified.paths["file"].read_text() == "replacement"
    assert results["rename_sibling"]["exit_code"] == 1
    assert "Operation not permitted" in results["rename_sibling"]["output"]
    assert not (certified.tmp / "moved-file").exists()


@pytest.mark.parametrize("fork", ["none", "all"])
def test_actual_ordinary_children_inherit_bounded_roots(certified, fork):
    grants = normalize_paths([str(certified.paths["grant"])])
    session, results = run_cli(certified, grants, fork=fork)
    assert_bounded(results)
    resumed, results = run_cli(certified, grants, session=session, fork=fork)
    assert resumed == session
    assert_bounded(results)
    records = [[json.loads(line) for line in path.read_text().splitlines()]
               for path in certified.paths["codex-home"].joinpath("sessions").rglob("*.jsonl")]
    children = [rows for rows in records if rows[0]["payload"].get("parent_thread_id") == session]
    assert len(children) == 2
    for rows in children:
        context = next(row["payload"] for row in rows if row["type"] == "turn_context")
        policy = context["sandbox_policy"]
        assert policy["type"] == "workspace-write"
        assert str(certified.paths["grant"].resolve()) in policy["writable_roots"]
        assert str(certified.paths["unrelated"].resolve()) not in policy["writable_roots"]
        assert policy["network_access"] is False
        assert policy["exclude_slash_tmp"] is policy["exclude_tmpdir_env_var"] is True
    # Plain CLI inheritance does not certify Polybridge's separately pinned adapter.
    from polybridge.backends.codex_native import CodexNativeAdapter
    parent = SimpleNamespace(backend="codex", freedom="write_in_repo", network=False,
                             model="gpt-6.1-sol", reasoning_effort=None, max_turns=None)
    assert CodexNativeAdapter().eligible(parent, {"model": parent.model},
        {"freedom": parent.freedom, "network": False, "backend_version": VERSION}) is not None
