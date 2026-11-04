"""Bounded actual-child fixture using delivered prose only; no engine state/oracles.

The raw ledger is untrusted fixture evidence. Public results and external process
identity must corroborate it. Reported cost/model are synthetic provider fields.
"""
import hashlib
import json
import math
import os
from pathlib import Path
import select
import sys
import time


def parse_spec(text, marker):
    if marker not in text:
        raise ValueError('unsupported fixture prompt: missing ' + marker)
    value, _ = json.JSONDecoder().raw_decode(text.split(marker, 1)[1].lstrip())
    if not isinstance(value, dict):
        raise ValueError('fixture specification must be an object')
    return value


def decision_from_prompt(text, case_spec=None):
    """Pure protocol fixture: select only currently delivered continuation IDs."""
    case_spec = case_spec or {}
    try:
        delivery = json.loads(text)
    except json.JSONDecodeError:
        raise ValueError('unsupported workflow prompt: requires optimized JSON delivery') from None
    if not isinstance(delivery, dict) or not isinstance(delivery.get('checkpoint'), dict):
        raise ValueError('unsupported workflow prompt: missing checkpoint')
    checkpoint = delivery['checkpoint']
    acknowledgement = delivery.get('delivery', {}).get('acknowledgement')
    if not isinstance(acknowledgement, dict):
        raise ValueError('unsupported workflow prompt: missing delivery acknowledgement')
    decision_id = checkpoint.get('decision_id')
    if not isinstance(decision_id, str) or not decision_id:
        raise ValueError('missing issued decision identity')
    base = {'decision_id': decision_id, 'context_ack': acknowledgement,
            'reason': 'Bounded synthetic conformance fixture using delivered choices'}
    actions = checkpoint.get('valid_actions', [])
    question = case_spec.get('needs_input_question')
    answer_token = case_spec.get('answer_token')
    # The caller authors a unique token. Resume must actually deliver it in the
    # current checkpoint; no stored engine state or fabricated answer is used.
    question_node = case_spec.get('question_node_id')
    question_checkpoint = question_node is None or checkpoint.get('current_stage', {}).get('node_id') == question_node
    if question and question_checkpoint and 'needs_input' in actions:
        if not isinstance(answer_token, str) or not answer_token:
            raise ValueError('needs-input fixture requires an authored answer token')
        if answer_token not in json.dumps(checkpoint, ensure_ascii=False):
            return {**base, 'action': 'needs_input', 'question': question}
    if 'complete' in actions:
        return {**base, 'action': 'complete'}
    choices = checkpoint.get('valid_continuations')
    if 'continue' not in actions or not isinstance(choices, list) or not choices:
        raise ValueError('unsupported workflow checkpoint: no offered completion/continuation')
    instructions = checkpoint.get('node_instructions', {})
    if not isinstance(instructions, dict):
        raise ValueError('unsupported workflow node instructions')

    def entry(choice, depth=0):
        if depth > 12 or not isinstance(choice, dict):
            raise ValueError('unsupported continuation nesting')
        cid = choice.get('continuation_id')
        if not isinstance(cid, str) or not cid:
            raise ValueError('missing issued continuation identity')
        result = {'continuation_id': cid}
        branches = choice.get('branch_continuations')
        if branches:
            if not isinstance(branches, list) or len(branches) > 32:
                raise ValueError('unsupported branch bundle')
            result['branch_assignments'] = [entry(branch, depth + 1) for branch in branches]
            if choice.get('branch_selection') == 'orchestrator':
                result['selection_reason'] = 'Select every issued branch for synthetic conformance'
        if choice.get('requires_prompt'):
            prose = instructions.get(choice.get('node_id'))
            if isinstance(prose, dict) and prose.get('mode') == 'full':
                prose = prose.get('value')
            if not isinstance(prose, str):
                raise ValueError('unsupported missing/referenced node instructions')
            # Assignment copies authored node instructions, including worker mode;
            # never searches graph internals or uses implementation renderers.
            scope = checkpoint.get('workflow_run_id')
            if not isinstance(scope, str) or not scope:
                raise ValueError('missing delivered workflow run identity')
            result['prompt'] = prose + '\nS09_DELIVERED_SCOPE: ' + json.dumps({'workflow_run_id': scope})
            if 'workflow' not in choice:
                result['session_mode'] = 'fresh'
            elif choice.get('child_session_policy') == 'agent_decides':
                result.update(child_session_mode='fresh',
                              child_session_reason='Fresh synthetic child conversation')
        return result

    if len(choices) != 1:
        raise ValueError('unsupported conditional routing: fixture requires one issued choice')
    return {**base, 'action': 'continue', 'next': [entry(choices[0])]}


def _hold(spec, receive, emit):
    gate = Path(spec['gate'])
    work = Path(os.environ['S08_WORKSPACE']).resolve()
    if gate.parent.resolve() != work or gate.name in ('', '.', '..'):
        raise ValueError('gate outside fixture workspace')
    bound = spec.get('hold_seconds', 120)
    if type(bound) not in (int, float) or not math.isfinite(bound) or not 0 < bound <= 120:
        raise ValueError('hold bound must be positive and <=120 seconds')
    emit({'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': 'fixture ready'}]}})
    deadline = time.monotonic() + bound
    input_open = True
    while not gate.is_file():
        if time.monotonic() >= deadline:
            raise TimeoutError('fixture gate timeout')
        ready, _, _ = select.select([0] if input_open else [], [], [], min(.1, max(0, deadline-time.monotonic())))
        if ready and receive(deadline) is None:
            if not spec.get('survive_eof', False):
                return False
            input_open = False
    return True


def _actor(spec, assignment):
    if not os.environ.get('HARFLOW_TASK_ID'):
        raise ValueError('actor requires engine-provided managed identity')
    sys.path.insert(0, str(Path(os.environ['S08_ARCHIVE_SRC']).parent))
    from conformance.process import run_process
    from conformance.oracles import read_regular, strict_json, validate_version_info, validate_envelope
    work = Path(os.environ['S08_WORKSPACE'])
    actor_dir = work / ('actor-' + str(os.getpid()))
    actor_dir.mkdir(exist_ok=False)

    def call(argv, name):
        result = run_process([os.environ['S08_PYTHON_SHIM'], *argv], cwd=work,
            env=dict(os.environ), artifact_dir=actor_dir / name, timeout=10, capture_limit=65536)
        if (result['timed_out'] or result['capture_exceeded'] or not result['pipes_drained']
                or result.get('returncode') is None or result.get('cleanup') not in
                ('group_signalled', 'group_absent', 'group_quiescent_os_observed')):
            raise ValueError('actor outcome unknown; no replay')
        document = strict_json(read_regular(result['streams']['stdout']['path']))
        validate_envelope(document, 'harflow.version-info' if argv[0] == 'version-info' else 'harflow.ctl')
        return result, document

    probe, document = call(['version-info', '--json'], 'probe')
    if probe['returncode'] != 0:
        raise ValueError('actor handshake failed')
    operation = spec.get('operation')
    caps = ('ctl.v1', 'statistics-page.v1') if operation == 'statistics-page' else ('ctl.v1',)
    validate_version_info(document, os.environ['HARFLOW_STATE_ROOT'], caps)
    if operation == 'statistics-page':
        from datetime import datetime
        for field in ('since', 'before'):
            value = spec.get(field)
            if not isinstance(value, str):
                raise ValueError('statistics actor requires explicit since/before')
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError('statistics actor requires timezone-aware bounds')
        argv = ['statistics-page', '--since', spec['since'], '--before', spec['before'],
                '--limit', '1', '--json']
    elif operation == 'workflow-status' and isinstance(spec.get('target_run'), str) and spec['target_run']:
        target = spec['target_run']
        if target == 'self':
            if 'S09_DELIVERED_SCOPE:' not in assignment:
                raise ValueError('missing delivered workflow scope')
            scope = parse_spec('S09_DELIVERED_SCOPE:' +
                               assignment.rsplit('S09_DELIVERED_SCOPE:', 1)[1],
                               'S09_DELIVERED_SCOPE:')
            target = scope.get('workflow_run_id')
            if not isinstance(target, str) or not target:
                raise ValueError('missing delivered workflow scope')
        argv = ['workflow-status', target, '--json']
    else:
        raise ValueError('unsupported managed actor operation')
    call(argv, 'capture')


def main():
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from client import register
    handle = register(os.environ['S08_OBSERVER_SOCKET'], os.environ['S08_OBSERVER_NONCE'],
                      'fake', source_profile='s09')
    session = sys.argv[sys.argv.index('--session-id') + 1]
    ledger = None
    try:
        ledger = open(Path(os.environ['S08_FAKE_LEDGER']) / (str(os.getpid()) + '.jsonl'), 'x', buffering=1)
        total = 0

        def record(direction, raw):
            nonlocal total
            row = (json.dumps({'direction': direction, 'bytes': len(raw),
                'sha256': hashlib.sha256(raw).hexdigest(), 'raw_hex': raw.hex()}) + '\n').encode()
            total += len(row)
            if total > 1048576:
                raise ValueError('fixture raw ledger budget exceeded')
            ledger.write(row.decode())

        def emit(value):
            raw = (json.dumps(value, allow_nan=False) + '\n').encode()
            record('stdout', raw)
            os.write(1, raw)

        def receive(deadline):
            raw = bytearray()
            while time.monotonic() < deadline:
                if not select.select([0], [], [], min(.1, max(0, deadline-time.monotonic())))[0]:
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

        frame = receive(time.monotonic() + 30)
        if not isinstance(frame, dict) or frame.get('type') != 'input' or not isinstance(frame.get('text'), str):
            raise ValueError('fixture requires engine initial input')
        assignment = frame['text']
        case = json.loads(os.environ.get('S09_CASE_SPEC', '{}'))
        try:
            decoded = json.loads(assignment)
        except json.JSONDecodeError:
            decoded = None
        workflow = isinstance(decoded, dict) and 'checkpoint' in decoded
        worker = not workflow and 'S09_WORKER_SPEC:' in assignment
        if workflow:
            spec = case
            text = json.dumps(decision_from_prompt(assignment, case))
        else:
            if worker:
                spec = parse_spec(assignment, 'S09_WORKER_SPEC:')
            elif 'S08_FIXTURE_SPEC:' in assignment:
                spec = parse_spec(assignment, 'S08_FIXTURE_SPEC:')
            elif isinstance(decoded, dict):
                spec = decoded
            else:
                raise ValueError('unsupported fixture prompt shape')
            text = spec.get('text', 'fixture complete')
        mode = spec.get('mode', 'complete')
        if mode not in ('complete', 'nonzero', 'hold', 'actor', 'managed_actor', 'invalid'):
            raise ValueError('unsupported fixture mode')
        init = {'type': 'system', 'subtype': 'init', 'session_id': session}
        model = spec.get('reported_model', case.get('reported_model'))
        cost = spec.get('reported_cost', case.get('reported_cost'))
        if model is not None:
            if not isinstance(model, str) or not model or len(model) > 200:
                raise ValueError('invalid synthetic reported model')
            init['model'] = model
        if cost is not None and (type(cost) not in (int, float) or not math.isfinite(cost) or cost < 0):
            raise ValueError('invalid synthetic reported cost')
        emit(init)
        if mode == 'hold' and not _hold(spec, receive, emit):
            return 0
        if mode in ('actor', 'managed_actor'):
            _actor(spec, assignment)
        if worker:
            text = json.dumps({'status': 'succeeded', 'result': {'summary': text}, 'evidence': []})
        if mode == 'invalid':
            text = 'deliberately invalid synthetic decision'
        emit({'type': 'assistant', 'message': {'content': [{'type': 'text', 'text': text}]}})
        result = {'type': 'result', 'subtype': 'success', 'result': text, 'is_error': False}
        if cost is not None:
            result['total_cost_usd'] = cost
        emit(result)
        return 7 if mode == 'nonzero' else 0
    finally:
        if ledger is not None:
            ledger.close()
        handle.close()


if __name__ == '__main__':
    raise SystemExit(main())
