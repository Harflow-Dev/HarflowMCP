"""Bounded model-free Codex effective workspace configuration witness.

Only initialize/initialized/config/read are ever sent. No thread or model turn is
created. Full native configuration is bounded in memory, then discarded; only
workspace sandbox fields and their source metadata are retained.
"""
from __future__ import annotations

import copy
import json
import os
import re
from pathlib import Path
import selectors
import subprocess
import time

from .conversation import ConversationRefused, frame_bytes

FIELDS = ('writable_roots', 'network_access', 'exclude_slash_tmp', 'exclude_tmpdir_env_var')
MAX_BYTES = 1024 * 1024


def filter_configuration(result):
    if type(result) is not dict or type(result.get('config')) is not dict or type(result.get('origins')) is not dict:
        raise ConversationRefused('Codex effective configuration/source witness missing')
    namespace = result['config'].get('sandbox_workspace_write')
    if type(namespace) is not dict or any(key not in namespace for key in FIELDS):
        raise ConversationRefused('Codex workspace configuration fields missing')
    roots = namespace['writable_roots']
    if (type(roots) is not list or len(roots) > 128
            or any(type(path) is not str or len(path) > 4096 or not Path(path).is_absolute() for path in roots)
            or any(type(namespace[key]) is not bool for key in FIELDS[1:])):
        raise ConversationRefused('Codex workspace configuration has unqualified values')
    origins = {}
    for key, metadata in result['origins'].items():
        if type(key) is not str:
            raise ConversationRefused('Codex configuration source key invalid')
        if key == 'sandbox_workspace_write' or key.startswith('sandbox_workspace_write.'):
            if (type(metadata) is not dict or type(metadata.get('name')) is not dict
                    or type(metadata.get('version')) is not str):
                raise ConversationRefused('Codex workspace configuration source metadata missing')
            value = copy.deepcopy(metadata)
            if value['name'].get('type') == 'sessionFlags':
                # This layer also includes unrelated developer/model pins added
                # after scratch allocation. Root-namespace pins are bound by the
                # independently validated Invocation and exact returned values.
                value = {'name': value['name']}
            origins[key] = value
    if not origins:
        raise ConversationRefused("Codex workspace configuration source witness absent")
    if len(origins) > 32:
        raise ConversationRefused('Codex configuration source metadata exceeds its bound')
    return {'workspace': {key: copy.deepcopy(namespace[key]) for key in FIELDS}, 'origins': origins}


def original_pins(invocation):
    pins = []
    args = invocation.argv
    end = args.index('--') if '--' in args else len(args)
    for index in range(end - 1):
        if args[index] == '-c':
            pins.extend(('-c', args[index + 1]))
    return pins


def baseline_argv(invocation, sandbox):
    return [invocation.argv[0], 'app-server', '--listen', 'stdio://', *original_pins(invocation),
            '-c', 'sandbox_mode=' + json.dumps(sandbox), '-c', 'approvals_reviewer="user"']


def read_configuration(argv, *, cwd, env, version, timeout=15.0):
    """Fresh effective configuration under exact source pins, outside leaf locks."""
    if not Path(cwd).is_absolute() or not 0 < timeout <= 30:
        raise ConversationRefused('Codex configuration witness requires bounded absolute cwd')
    process = subprocess.Popen(argv, cwd=cwd, env={**os.environ, **env}, stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    selector = selectors.DefaultSelector()
    pending = bytearray()
    total = 0
    deadline = time.monotonic() + timeout
    for stream in (process.stdout, process.stderr):
        os.set_blocking(stream.fileno(), False)
        selector.register(stream, selectors.EVENT_READ)
    def send(frame):
        process.stdin.write(frame_bytes(frame))
        process.stdin.flush()
    def response(request_id):
        nonlocal total
        while time.monotonic() < deadline:
            while b'\n' in pending:
                line, _, remainder = pending.partition(b'\n')
                pending[:] = remainder
                frame = json.loads(line)
                if type(frame) is not dict:
                    raise ConversationRefused('Codex configuration witness frame is invalid')
                if 'method' in frame:
                    if 'id' in frame or str(frame['method']).startswith(('turn/', 'item/')):
                        raise ConversationRefused('unexpected native request/activity in model-free configuration witness')
                elif frame.get('id') == request_id:
                    if 'error' in frame or type(frame.get('result')) is not dict:
                        raise ConversationRefused('Codex configuration read RPC refused')
                    return frame['result']
            events = selector.select(max(0, deadline - time.monotonic()))
            if not events:
                break
            for key, _ in events:
                chunk = os.read(key.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                total += len(chunk)
                if total > MAX_BYTES:
                    raise ConversationRefused('Codex configuration witness output exceeds its bound')
                if key.fileobj is process.stdout:
                    pending.extend(chunk)
            if not selector.get_map():
                break
        raise ConversationRefused('Codex configuration witness deadline/EOF before response')
    result = None
    try:
        send({'id': 1, 'method': 'initialize', 'params': {'clientInfo': {'name': 'harflow_config_witness', 'version': '1'},
                                                       'capabilities': {'experimentalApi': True}}})
        initialized = response(1)
        from .codex_qualification import parse_user_agent
        user_agent = initialized.get('userAgent')
        observed_version = parse_user_agent(user_agent)
        if observed_version is None and type(user_agent) is str:
            desktop = re.match(r'^Codex Desktop/(\d+\.\d+\.\d+) \(', user_agent)
            observed_version = desktop.group(1) if desktop else None
        if observed_version != version:
            raise ConversationRefused('Codex configuration witness version mismatch')
        send({'method': 'initialized'})
        send({'id': 2, 'method': 'config/read', 'params': {'cwd': cwd, 'includeLayers': False}})
        result = filter_configuration(response(2))
    except ConversationRefused:
        raise
    except (OSError, ValueError, TypeError, KeyError) as exc:
        raise ConversationRefused('Codex effective configuration observation failed') from exc
    finally:
        process.stdin.close()
        cleanup_deadline = time.monotonic() + 3
        while selector.get_map() and time.monotonic() < cleanup_deadline:
            for key, _ in selector.select(max(0, cleanup_deadline - time.monotonic())):
                if not os.read(key.fileobj.fileno(), 65536):
                    selector.unregister(key.fileobj)
        drained = not selector.get_map()
        try:
            process.wait(timeout=max(.001, cleanup_deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=2)
        selector.close()
        process.stdout.close()
        process.stderr.close()
        if result is not None and (not drained or process.returncode != 0):
            raise ConversationRefused('Codex configuration witness process exit/drain not qualified')
    return result
