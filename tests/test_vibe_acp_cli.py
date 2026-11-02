"""Installed Vibe ACP 3.0.3 qualification, localhost scripted provider only."""
import json
import os
import queue
import shutil
import subprocess
import threading
import time

import pytest

from harflow_mcp.backends import Accumulator
from harflow_mcp.backends import vibe_acp as acp
from test_vibe_cli import provider

pytestmark = [pytest.mark.cli_integration, pytest.mark.skipif(
    os.environ.get("HARFLOW_CLI_INTEGRATION") != "1", reason="requires installed Vibe and localhost listener")]


def environment(tmp_path, url):
    home, config, repo = (tmp_path / n for n in ("home", "vibe", "repo"))
    for directory in (home, config, repo):
        directory.mkdir(exist_ok=True)
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "TMPDIR", "LANG", "SYSTEMROOT")}
    env.update(HOME=str(home), VIBE_HOME=str(config), DO_NOT_TRACK="1", MISTRAL_API_KEY="local-only-placeholder", HARFLOW_LOCAL_TEST_KEY="local-only-placeholder")
    (config / "config.toml").write_text(f'''active_model="local"
api_retry_max_elapsed_time=0.1
enable_connectors=false
enable_telemetry=false
[[providers]]
name="local"
api_base="{url}"
api_key_env_var="HARFLOW_LOCAL_TEST_KEY"
[[models]]
name="local"
alias="local"
provider="local"
thinking="off"
[[models]]
name="measured-selected-model"
alias="selected"
provider="local"
thinking="off"
''')
    return env, repo


def drive(env, repo, *, decision="approve", session=None, model=None, cap=3, mode="ask", cancel=False, write=True):
    binary = shutil.which("vibe-acp")
    if not binary:
        pytest.skip("vibe-acp unavailable")
    ctx = {"transport": acp.TRANSPORT, "version": acp.VERSION, "cwd": str(repo), "prompt": "isolated ACP measurement", "freedom": "read_only", "mode": mode,
           "model": model, "session_id": session, "max_turns": cap, "write": write, "caller_approvals": True, "trust": "trust_cwd"}
    if mode != "ask":
        ctx['freedom'] = next(k for k, v in acp.MODES.items() if v == mode)
    acc = Accumulator()
    acc.stream_state['control_context'] = ctx
    child_env = {**env, **({"VIBE_ACTIVE_MODEL": model} if model else {})}
    process = subprocess.Popen([binary], cwd=repo, env=child_env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    frames = queue.Queue()
    errors = []
    def reader():
        for line in process.stdout:
            try:
                frames.put(json.loads(line))
            except Exception as exc:
                errors.append((line, exc))
    thread = threading.Thread(target=reader, daemon=True)
    thread.start()
    history, requests = [], []
    def send(data):
        process.stdin.write(data)
        process.stdin.flush()
    try:
        for data in acp.initial_frames(acc):
            send(data)
        deadline = time.monotonic() + 40
        while not acp.finished(acc):
            try:
                frame = frames.get(timeout=max(.01, deadline-time.monotonic()))
            except queue.Empty:
                raise AssertionError(f"ACP timeout; history={history}; parser={errors}") from None
            history.append(frame)
            state = acp.state_for(acc)
            acp.step(state, frame)
            if acp.frame_kind(frame) == "request":
                request = acp.decode(frame, {"state": state})
                requests.append(request)
                if cancel:
                    send(acp.cancel_frame(state))
                    send(acp.encode(request, "cancel"))
                    cancel = False
                else:
                    send(acp.encode(request, "reject" if request.get("system_rejection") else decision(request) if callable(decision) else decision))
            acp.ingest(frame, acc)
            acp.normalize(frame, acc)
            for data in acp.turn_frames(acc):
                send(data)
        return acc, history, requests
    finally:
        process.stdin.close()
        process.wait(timeout=10)
        thread.join(timeout=2)
        stderr = process.stderr.read().decode(errors="replace")
        assert process.returncode == 0, stderr


@pytest.mark.parametrize("decision", ["approve", "reject", "cancel"])
def test_actual_acp_permission_once_and_load(tmp_path, decision):
    with provider("denial-shape", ("bash", {"command": "touch exact-marker"})) as (url, calls):
        env, repo = environment(tmp_path, url)
        acc, history, requests = drive(env, repo, decision=decision)
        assert acp.state_for(acc).observed_version == "3.0.3"
        assert acp.state_for(acc).terminal['stop_reason'] == 'end_turn'
        assert len(requests) == 1
        assert requests[0]['action'] == {'tool_name': 'bash', 'input': {'command': 'touch exact-marker'}}
        assert (repo / 'exact-marker').exists() == (decision == 'approve')
        assert len(calls) >= 2
        resumed, _, _ = drive(env, repo, session=acc.session_id)
        assert resumed.session_id == acc.session_id
        assert resumed.summary == 'LOCAL ANSWER'


def test_actual_acp_model_selected_provider_request(tmp_path):
    with provider('text') as (url, calls):
        env, repo = environment(tmp_path, url)
        import tomllib
        config_path = __import__('pathlib').Path(env['VIBE_HOME']) / 'config.toml'
        original = tomllib.loads(config_path.read_text())
        acc, _, _ = drive(env, repo, model='selected')
        assert tomllib.loads(config_path.read_text()) == original
        assert acc.summary == 'LOCAL ANSWER'
        assert calls and all(c['model'] == 'measured-selected-model' for c in calls)


def test_actual_acp_turn_cap(tmp_path):
    with provider('cap') as (url, calls):
        env, repo = environment(tmp_path, url)
        acc, _, _ = drive(env, repo, cap=1)
        assert acp.state_for(acc).terminal['stop_reason'] == 'max_turn_requests'
        assert len([c for c in calls if any(m.get('role') == 'user' and m.get('content') == 'isolated ACP measurement' for m in c['messages'])]) == 1


@pytest.mark.parametrize('mode', ['ask', 'accept-edits', 'auto-approve'])
def test_actual_acp_mode_selection(tmp_path, mode):
    with provider('denial-shape', ('bash', {'command': 'touch mode-marker'})) as (url, calls):
        env, repo = environment(tmp_path, url)
        acc, _, requests = drive(env, repo, mode=mode)
        assert acp.state_for(acc).terminal['stopReason'] == 'end_turn'
        assert (repo / 'mode-marker').exists()
        if mode == 'auto-approve':
            assert not requests


def test_actual_acp_cancel_turn(tmp_path):
    with provider('denial-shape', ('bash', {'command': 'touch cancelled-marker'})) as (url, calls):
        env, repo = environment(tmp_path, url)
        acc, _, _ = drive(env, repo, cancel=True)
        assert not (repo / 'cancelled-marker').exists()
        assert acp.state_for(acc).terminal['stopReason'] == 'cancelled'


def test_actual_acp_exact_cwd_trust_honors_project_never(tmp_path):
    with provider('denial-shape', ('bash', {'command': 'touch project-denied'})) as (url, calls):
        env, repo = environment(tmp_path, url)
        subprocess.run(['git', 'init', '-q', str(repo)], check=True, capture_output=True)
        (repo / '.vibe').mkdir()
        (repo / '.vibe' / 'config.toml').write_text('[tools.bash]\npermission="never"\n')
        acc, history, requests = drive(env, repo)
        assert not requests, [f for f in history if 'result' in f]
        assert not (repo / 'project-denied').exists()
        assert acp.state_for(acc).terminal['stopReason'] == 'end_turn'
        trusted_files = [p for p in __import__('pathlib').Path(env['VIBE_HOME']).glob('*trust*') if p.is_file()]
        assert trusted_files and any(str(repo) in p.read_text() for p in trusted_files)


def test_actual_acp_parallel_sessions_are_isolated(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    def one(index):
        root = tmp_path / str(index)
        root.mkdir()
        with provider('denial-shape', ('bash', {'command': f'touch marker-{index}'})) as (url, calls):
            env, repo = environment(root, url)
            acc, _, requests = drive(env, repo)
            assert (repo / f'marker-{index}').exists()
            assert not (repo / f'marker-{1-index}').exists()
            return acc.session_id, requests[0]['action']['input']['command']
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(one, [0, 1]))
    assert results[0][0] != results[1][0]
    assert results[0][1] != results[1][1]


@pytest.mark.parametrize('tool,write', [('write_file', True), ('write_file', False), ('read_file', False), ('edit', True)])
def test_actual_acp_file_permissions_original_input(tmp_path, tool, write):
    # Sensitive outside path forces the native approval gate in ask mode.
    outside = tmp_path / '.env'
    outside.write_text('old')
    raw = {'file_path' if tool == 'edit' else 'path': str(outside)}
    if tool == 'write_file':
        raw['content'] = 'exact-content'
    elif tool == 'edit':
        raw.update(old_string='old', new_string='exact-edit')
    with provider('denial-shape', (tool, raw)) as (url, calls):
        env, repo = environment(tmp_path, url)
        acc, _, requests = drive(env, repo, write=write)
        assert requests, tool
        action = requests[0]['action']
        assert action['tool_name'] == tool
        assert action['input']['filePath'] == str(outside)
        if tool == 'write_file':
            assert action['input']['content'] == 'exact-content'
            assert outside.read_text() == ('exact-content' if write else 'old')
        elif tool == 'edit':
            assert action['input']['oldString'] == 'old' and action['input']['newString'] == 'exact-edit'
            assert outside.read_text() == 'exact-edit'
        else:
            assert outside.read_text() == 'old'
        assert acp.state_for(acc).terminal['stopReason'] == 'end_turn'



def test_actual_two_tool_calls_same_session_preserve_approval_identity(tmp_path):
    import http.server
    from contextlib import contextmanager
    calls = []
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            calls.append(body)
            tool_request = bool(body.get('tools')) and not any(m.get('role') == 'tool' for m in body.get('messages', []))
            delta = {'role': 'assistant', 'content': 'LOCAL ANSWER'}
            if tool_request:
                delta = {'role': 'assistant', 'tool_calls': [
                    {'index': index, 'id': f'parallel-{index}', 'type': 'function', 'function': {'name': 'bash', 'arguments': json.dumps({'command': f'touch {name}'})}}
                    for index, name in enumerate(['allowed-marker', 'refused-marker'])]}
            self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.end_headers()
            for part, finish in [(delta, None), ({}, 'tool_calls' if tool_request else 'stop')]:
                chunk = {'id': 'local', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'local', 'choices': [{'index': 0, 'delta': part, 'finish_reason': finish}]}
                self.wfile.write(('data: ' + json.dumps(chunk) + '\n\n').encode())
            usage = {'id': 'local', 'object': 'chat.completion.chunk', 'created': 1, 'model': 'local', 'choices': [], 'usage': {'prompt_tokens': 10, 'completion_tokens': 10, 'total_tokens': 20}}
            self.wfile.write(('data: ' + json.dumps(usage) + '\n\n').encode())
            self.wfile.write(b'data: [DONE]\n\n')
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    try:
        env, repo = environment(tmp_path, f'http://127.0.0.1:{server.server_port}/v1')
        acc, _, requests = drive(env, repo, decision=lambda r: 'approve' if r['action']['input']['command'] == 'touch allowed-marker' else 'reject')
        assert (repo / 'allowed-marker').exists()
        assert not (repo / 'refused-marker').exists()
        assert len(requests) == 2 and len({r['tool_use_id'] for r in requests}) == 2
        assert {r['action']['input']['command'] for r in requests} == {'touch allowed-marker', 'touch refused-marker'}
        assert acp.state_for(acc).terminal['stopReason'] == 'end_turn'
    finally:
        server.shutdown(); server.server_close(); thread.join()
