"""Actual engine child with a bounded synthetic protocol and byte-hashed raw I/O."""
import hashlib
import json
import os
from pathlib import Path
import select
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
from client import register


def main():
    handle = register(os.environ['S08_OBSERVER_SOCKET'], os.environ['S08_OBSERVER_NONCE'], 'fake')
    session = sys.argv[sys.argv.index('--session-id') + 1]
    ledger_dir = Path(os.environ['S08_FAKE_LEDGER'])
    path = ledger_dir / (str(os.getpid()) + '.jsonl')
    ledger = open(path, 'x', buffering=1)
    total = 0
    def record(direction, raw):
        nonlocal total
        total += len(raw)
        if total > 1048576:
            raise ValueError('fixture I/O budget exceeded')
        ledger.write(json.dumps({'direction': direction, 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest(), 'raw_hex': raw.hex()}) + '\n')
    def emit(value):
        raw = (json.dumps(value) + '\n').encode()
        record('stdout', raw)
        os.write(1, raw)
    def receive(deadline):
        raw = bytearray()
        while time.monotonic() < deadline:
            ready, _, _ = select.select([0], [], [], min(.1, max(0, deadline-time.monotonic())))
            if not ready:
                continue
            chunk = os.read(0, 1)
            if not chunk:
                return None
            raw.extend(chunk)
            if len(raw) > 65536:
                raise ValueError('fixture input frame exceeded')
            if chunk == b'\n':
                record('stdin', bytes(raw))
                return json.loads(raw)
        raise TimeoutError('fixture bounded input timeout')
    try:
        frame = receive(time.monotonic() + 30)
        if not frame or frame.get('type') != 'input':
            raise ValueError('fixture requires engine initial input')
        assignment = frame['text']
        marker = 'S08_FIXTURE_SPEC:'
        if marker in assignment:
            spec, _ = json.JSONDecoder().raw_decode(assignment.split(marker, 1)[1].lstrip())
        else:
            spec = json.loads(assignment)
        mode, text = spec.get('mode', 'complete'), spec.get('text', 'fixture complete')
        if mode not in ('complete', 'nonzero', 'hold', 'approval', 'actor'):
            raise ValueError('unknown fixture mode')
        emit({'type': 'system', 'subtype': 'init', 'session_id': session})
        if mode == 'hold':
            emit({'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'fixture ready'}]}})
            gate = Path(spec['gate'])
            allowed = Path(os.environ['S08_WORKSPACE']).resolve()
            if gate.parent.resolve() != allowed:
                raise ValueError('gate outside fixture workspace')
            deadline = time.monotonic() + 45
            input_open = True
            while not gate.is_file():
                if time.monotonic() >= deadline:
                    raise TimeoutError('fixture gate timeout')
                ready, _, _ = select.select([0] if input_open else [], [], [], .1)
                if ready:
                    extra = receive(deadline)
                    if extra is None:
                        if not spec.get('survive_eof', False):
                            return 0
                        input_open = False
        elif mode == 'approval':
            sentinel = Path(os.environ['S08_WORKSPACE']) / 'approved-effects.txt'
            for key in ('A', 'B'):
                emit({'type': 'ask', 'id': key, 'command': 'fixture append fixed sentinel'})
                answer = receive(time.monotonic() + 30)
                if not answer or answer.get('id') != 's:' + key:
                    raise ValueError('fixture answer identity mismatch')
                if answer.get('answer') == 'approve':
                    with open(sentinel, 'a') as stream:
                        stream.write(key + '\n')
        elif mode == 'actor':
            # A genuine engine child, carrying the engine's managed identity,
            # invokes a public CLI operation. No lineage/auth gate is patched.
            if not os.environ.get('HARFLOW_TASK_ID'):
                raise ValueError('actor requires an engine-provided managed identity')
            sys.path.insert(0, str(Path(os.environ['S08_ARCHIVE_SRC']).parent))
            from conformance.process import run_process
            from conformance.oracles import read_regular, strict_json, validate_version_info
            work = Path(os.environ['S08_WORKSPACE'])
            def actor_call(args, name):
                result = run_process([os.environ['S08_PYTHON_SHIM'], *args], cwd=work,
                    env=dict(os.environ), artifact_dir=work / name, timeout=10, capture_limit=65536)
                if (result['timed_out'] or result['capture_exceeded'] or not result['pipes_drained']
                        or result.get('returncode') is None or result.get('cleanup') not in
                        ('group_signalled', 'group_absent', 'group_quiescent_os_observed')):
                    raise ValueError('actor outcome unknown; no replay')
                return result
            probe = actor_call(['version-info', '--json'], 'actor-probe')
            if probe['returncode'] != 0:
                raise ValueError('actor handshake failed')
            validate_version_info(strict_json(read_regular(probe['streams']['stdout']['path'])),
                os.environ['HARFLOW_STATE_ROOT'], ('ctl.v1',))
            actor_call(['approval-list', '--task-id', spec['target_task'], '--json'], 'actor-capture')
        emit({'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': text}]}})
        emit({'type': 'result', 'subtype': 'success', 'result': text, 'is_error': False})
        return 7 if mode == 'nonzero' else 0
    finally:
        ledger.close()
        handle.close()

if __name__ == '__main__':
    raise SystemExit(main())
