"""Protocol self-tests with fake transport; never execute an engine or actor command."""
import importlib.util
import json
import os
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def fake(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / 'conformance/s08'))
    spec = importlib.util.spec_from_file_location('independent_s08_fake', ROOT / 'conformance/s08/fake_agent.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    closed = []
    class Handle:
        def close(self): closed.append(True)
    monkeypatch.setattr(module, 'register', lambda *args: Handle())
    monkeypatch.setattr(sys, 'argv', ['fake', '--session-id','independent-session'])
    monkeypatch.setattr(sys, 'path', list(sys.path))
    for key, value in {'S08_OBSERVER_SOCKET':'stub-only','S08_OBSERVER_NONCE':'nonce',
                       'S08_FAKE_LEDGER':str(tmp_path),'S08_WORKSPACE':str(tmp_path),
                       'S08_ARCHIVE_SRC':str(tmp_path / 'src'),
                       'S08_PYTHON_SHIM':'/stub-only-shim', 'HARFLOW_STATE_ROOT':str(tmp_path / 'state')}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv('HARFLOW_TASK_ID', raising=False)
    output = []
    monkeypatch.setattr(module.os, 'write', lambda fd, raw: output.append(raw) or len(raw))
    monkeypatch.setattr(module.select, 'select', lambda readers, *args: (readers, [], []))
    def feed(specification):
        raw = json.dumps({'type':'input','text':specification}).encode() + b'\n'
        data = iter(bytes([byte]) for byte in raw)
        monkeypatch.setattr(module.os, 'read', lambda *args: next(data,b''))
    return module, feed, output, closed


def test_marker_extracts_one_json_spec_and_ignores_context_suffix(fake):
    module, feed, output, closed = fake
    feed('controlled prefix\nS08_FIXTURE_SPEC: {"text":"verbatim result"}\ncaller context suffix')
    assert module.main() == 0
    frames = [json.loads(raw) for raw in output]
    assert frames[0]['session_id'] == 'independent-session'
    assert frames[-1]['result'] == 'verbatim result'
    assert closed == [True]


@pytest.mark.parametrize('assignment', ['S08_FIXTURE_SPEC: {bad}', '{"mode":"arbitrary-command"}'])
def test_invalid_spec_or_unknown_mode_refuses_before_result(fake, assignment):
    module, feed, output, closed = fake
    feed(assignment)
    with pytest.raises(ValueError):
        module.main()
    assert not any(json.loads(raw).get('type') == 'result' for raw in output)
    assert closed == [True]


def test_hold_eof_default_exits_without_terminal_success_claim(fake, tmp_path):
    module, feed, output, closed = fake
    feed(json.dumps({'mode':'hold','gate':str(tmp_path / 'gate')}))
    assert module.main() == 0
    assert [json.loads(raw)['type'] for raw in output] == ['system','assistant']
    assert closed == [True]


def test_survive_eof_removes_closed_input_and_waits_for_fixed_gate(fake, tmp_path, monkeypatch):
    module, feed, output, closed = fake
    gate = tmp_path / 'gate'
    feed(json.dumps({'mode':'hold','gate':str(gate),'survive_eof':True,'text':'gate completed'}))
    empty_waits = []
    def ready(readers, *args):
        if not readers:
            empty_waits.append(True)
            gate.write_text('owned release')
        return readers, [], []
    monkeypatch.setattr(module.select, 'select', ready)
    assert module.main() == 0
    assert empty_waits == [True]
    assert json.loads(output[-1])['result'] == 'gate completed'
    assert closed == [True]


def test_actor_without_managed_identity_refuses_any_subprocess(fake, monkeypatch):
    module, feed, output, closed = fake
    import conformance.process
    monkeypatch.setattr(conformance.process, 'run_process', lambda *args, **kwargs: pytest.fail('unmanaged actor spawned'))
    feed('{"mode":"actor","target_task":"owned-target"}')
    with pytest.raises(ValueError, match='managed identity'):
        module.main()
    assert closed == [True]


@pytest.mark.parametrize('fault', [None,'timed_out','missing_returncode','unknown_cleanup','missing_cleanup'])
def test_actor_only_fresh_handshake_then_exact_read_and_never_replays(fake, tmp_path, monkeypatch, fault):
    module, feed, output, closed = fake
    import conformance.process
    monkeypatch.setenv('HARFLOW_TASK_ID','owned-managed-identity')
    document = dict(product='harflow',format='harflow.version-info',v=1,product_version='0.0.7',
        component='engine', contracts={k:dict(format=v,v=1) for k,v in
            [('ctl','harflow.ctl'),('setup','harflow.setup'),('events','harflow.event')]},
        capabilities=['ctl.v1','version-info.v1'],os='darwin',architecture='arm64',state_root=str(tmp_path / 'state'))
    stdout = tmp_path / 'handshake.json'
    stdout.write_text(json.dumps(document))
    calls = []
    def run(argv, **kwargs):
        calls.append((argv,kwargs))
        record = {'timed_out':fault == 'timed_out','capture_exceeded':False,'pipes_drained':True,
                  'returncode':0,'cleanup':'group_quiescent_os_observed',
                  'streams':{'stdout':{'path':str(stdout)}}}
        if fault == 'missing_returncode':
            record['returncode'] = None
        elif fault == 'unknown_cleanup':
            record['cleanup'] = 'unknown'
        elif fault == 'missing_cleanup':
            record.pop('cleanup')
        return record
    monkeypatch.setattr(conformance.process,'run_process',run)
    feed('{"mode":"actor","target_task":"owned-target"}')
    if fault is not None:
        with pytest.raises(ValueError,match='no replay'):
            module.main()
        assert len(calls) == 1
    else:
        assert module.main() == 0
        assert [row[0] for row in calls] == [
            ['/stub-only-shim','version-info','--json'],
            ['/stub-only-shim','approval-list','--task-id','owned-target','--json']]
    for _, kwargs in calls:
        assert kwargs['timeout'] == 10 and kwargs['capture_limit'] == 65536
        assert kwargs['env']['HARFLOW_TASK_ID'] == 'owned-managed-identity'
    assert closed == [True]
