"""Read-only stable-parent observation, independent of execution ownership."""
from types import SimpleNamespace
from pathlib import Path
import pytest
from harflow_mcp import supervision, ctl

START = 'Wed Oct 28 09:00:00 2026'
ORIGIN = {'pid': 11, 'start_time': START}
PARENT = {'pid': 22, 'start_time': START}
SELF = {'pid': 33, 'start_time': START}

@pytest.fixture
def observation(monkeypatch):
    monkeypatch.setattr(supervision.identity, 'own_identity', lambda: SELF)
    monkeypatch.setattr(supervision.identity, 'identity_check', lambda value: 'dead' if value == ORIGIN else 'alive')
    monkeypatch.setattr(supervision, 'evidence', lambda *a, **kw: {
        'owner_state': 'alive', 'process_state': 'alive', 'capture_state': 'live',
        'live_input_state': 'open', 'last_captured_output_at': None})
    return SimpleNamespace(dispatch_origin=ORIGIN, owner={'pid': 44, 'start_time': START})

def test_default_ctl_suppresses_adoption_but_mcp_retains_shape(observation):
    value = supervision.ctl_fields(observation, Path('unused'))
    assert 'adopted_after_restart' not in value['supervision']
    assert value['supervision']['dispatch_origin_state'] == 'dead'
    adoption = supervision.fields(observation, Path('unused'))['supervision']['adopted_after_restart']
    assert adoption['current_server'] == SELF
    assert set(adoption) == {'at', 'previous_server', 'current_server'}

def test_parent_is_stable_and_not_mislabelled_server(observation):
    values = [supervision.ctl_fields(observation, Path('unused'), parent_observer={'identity': PARENT, 'state': 'alive'}) for _ in range(2)]
    assert values[0] == values[1]
    value = values[0]
    assert value['responding_server'] == SELF
    assert value['observing_process'] == PARENT
    adoption = value['supervision']['adopted_after_restart']
    assert set(adoption) == {'at', 'previous_server', 'observer_kind', 'observing_process'}
    assert adoption['observer_kind'] == 'parent_process'
    assert adoption['observing_process'] == PARENT
    assert adoption['at'] == supervision._observer_started_at(PARENT)

@pytest.mark.parametrize('state', ['dead', 'undecidable'])
def test_unqualified_parent_never_falls_back(observation, state):
    value = supervision.ctl_fields(observation, Path('unused'), parent_observer={'identity': None, 'state': state})
    assert value['observing_process'] is None
    assert value['observation_mode'] == 'unknown'
    assert 'adopted_after_restart' not in value['supervision']
    assert value['observer_state'] == state

@pytest.mark.parametrize('state', ['alive', 'dead', 'undecidable'])
def test_capture_qualifies_actual_parent(monkeypatch, state):
    monkeypatch.setattr(supervision.os, 'getppid', lambda: 22)
    monkeypatch.setattr(supervision.identity, 'capture', lambda pid, markers: {**PARENT, 'markers': markers} if pid == 22 else pytest.fail('not immediate parent'))
    monkeypatch.setattr(supervision.identity, 'identity_check', lambda value: state)
    result = supervision.capture_parent_observer()
    assert (result['identity'] is not None) == (state == 'alive')
    assert result['state'] == state

def test_reparent_during_qualification_fails_closed(monkeypatch):
    pids = iter([22, 23])
    monkeypatch.setattr(supervision.os, 'getppid', lambda: next(pids))
    monkeypatch.setattr(supervision.identity, 'capture', lambda *a: PARENT)
    monkeypatch.setattr(supervision.identity, 'identity_check', lambda *a: 'alive')
    assert supervision.capture_parent_observer() == {'identity': None, 'state': 'undecidable'}

def test_missing_capture_fails_closed(monkeypatch):
    monkeypatch.setattr(supervision.os, 'getppid', lambda: 22)
    monkeypatch.setattr(supervision.identity, 'capture', lambda *a: None)
    assert supervision.capture_parent_observer() == {'identity': None, 'state': 'undecidable'}

def test_option_available_only_for_read_status_commands():
    parser = ctl._build_parser()[0]
    for argv in [['status', 'abc', '--observer-parent'], ['status-batch', '--task-ids', 'abc', '--observer-parent']]:
        assert parser.parse_args(argv).observer_parent

def test_batch_qualifies_parent_once_and_shares_context(monkeypatch, capsys):
    from harflow_mcp import workflow_inspection
    context = {'identity': PARENT, 'state': 'alive'}
    captures, seen = [], []
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: Path('unused'))
    monkeypatch.setattr(workflow_inspection, 'managed_reader', lambda path: None)
    monkeypatch.setattr(supervision, 'capture_parent_observer', lambda: captures.append(True) or context)
    monkeypatch.setattr(ctl, '_status_snapshot', lambda path, task, managed, **kw: seen.append(kw['parent_observer']) or {'task_id': task})
    args = SimpleNamespace(task_ids='a'*32+','+'b'*32, json=True, observer_parent=True)
    assert ctl._cmd_status_batch(args) == 0
    assert captures == [True]
    assert len(seen) == 2 and all(value is context for value in seen)

@pytest.mark.parametrize('start', [None, '', 123, True, 'x' * 129])
def test_invalid_parent_start_is_not_identity(monkeypatch, start):
    monkeypatch.setattr(supervision.os, 'getppid', lambda: 22)
    monkeypatch.setattr(supervision.identity, 'capture', lambda *a: {**PARENT, 'start_time': start})
    monkeypatch.setattr(supervision.identity, 'identity_check', lambda *a: pytest.fail('invalid identity qualified'))
    assert supervision.capture_parent_observer() == {'identity': None, 'state': 'undecidable'}

def test_invalid_parent_check_result_fails_closed(monkeypatch):
    monkeypatch.setattr(supervision.os, 'getppid', lambda: 22)
    monkeypatch.setattr(supervision.identity, 'capture', lambda *a: PARENT)
    monkeypatch.setattr(supervision.identity, 'identity_check', lambda *a: 'unexpected')
    assert supervision.capture_parent_observer() == {'identity': None, 'state': 'undecidable'}

def test_adoption_ignores_polling_ctl_generation_but_tracks_parent_generation(observation, monkeypatch):
    def read(parent):
        return supervision.ctl_fields(observation, Path('unused'), parent_observer={'identity': parent, 'state': 'alive'})
    first = read(PARENT)
    monkeypatch.setattr(supervision.identity, 'own_identity', lambda: {**SELF, 'pid': 55})
    second = read(PARENT)
    assert first['responding_server'] != second['responding_server']
    assert first['supervision']['adopted_after_restart'] == second['supervision']['adopted_after_restart']
    next_parent = {**PARENT, 'start_time': 'Wed Oct 28 10:00:00 2026'}
    third = read(next_parent)
    assert third['supervision']['adopted_after_restart'] != second['supervision']['adopted_after_restart']
    assert third['supervision']['adopted_after_restart']['observing_process']['pid'] == PARENT['pid']

@pytest.mark.parametrize('parent', [None, {'identity': PARENT, 'state': 'alive'}, {'identity': None, 'state': 'undecidable'}])
def test_status_snapshot_real_store_forwarding(observation, monkeypatch, tmp_path, parent):
    from harflow_mcp import store, workflow_inspection
    from harflow_mcp.tasks import default_log_dir
    from test_ctl import make_record
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(workflow_inspection, 'guard_task_read', lambda *a: None)
    monkeypatch.setattr(workflow_inspection, 'decorate_tasks', lambda snapshots, path: snapshots)
    record = make_record(dispatch_origin={**ORIGIN, "markers": []}, status="running", exit_code=None, finished_at=None)
    log_dir = default_log_dir()
    store.write(log_dir, record)
    before = {p.name: p.read_bytes() for p in log_dir.rglob('*') if p.is_file() and not p.name.startswith('.listing')}
    snapshot = ctl._status_snapshot(log_dir, record.task_id, None, parent_observer=parent)
    assert snapshot['responding_server'] == SELF
    adoption = snapshot['supervision'].get('adopted_after_restart')
    if parent and parent['identity']:
        assert adoption['observing_process'] == PARENT
    else:
        assert adoption is None
    if parent and parent['identity'] is None:
        assert snapshot['observer_state'] == 'undecidable'
        assert 'could not be qualified alive' in snapshot['observation_note']
    assert before == {p.name: p.read_bytes() for p in log_dir.rglob('*') if p.is_file() and not p.name.startswith('.listing')}

def test_wrapper_is_the_immediate_parent_not_an_arbitrary_identity(monkeypatch):
    wrapper = {**PARENT, 'pid': 66}
    monkeypatch.setattr(supervision.os, 'getppid', lambda: 66)
    monkeypatch.setattr(supervision.identity, 'capture', lambda pid, markers: wrapper if pid == 66 else pytest.fail('ancestor walk'))
    monkeypatch.setattr(supervision.identity, 'identity_check', lambda value: 'alive')
    assert supervision.capture_parent_observer()['identity'] == wrapper
    parser = ctl._build_parser()[0]
    with pytest.raises(SystemExit):
        parser.parse_args(['status', 'abc', '--observer-identity', '{"pid":22}'])

def test_real_ctl_list_suppresses_ephemeral_adoption_but_mcp_brief_retains_it(observation, monkeypatch, tmp_path, capsys):
    import json
    from harflow_mcp import store, workflow_inspection
    from harflow_mcp.tasks import default_log_dir
    from test_ctl import make_record
    monkeypatch.setenv('HOME', str(tmp_path))
    monkeypatch.setattr(workflow_inspection, 'decorate_tasks', lambda snapshots, path: snapshots)
    monkeypatch.setattr(workflow_inspection, 'filter_task_reads', lambda entries, managed: entries)
    monkeypatch.setattr(workflow_inspection, 'managed_reader', lambda path: None)
    record = make_record(dispatch_origin={**ORIGIN, 'markers': []}, status='running', exit_code=None, finished_at=None)
    log_dir = default_log_dir()
    store.write(log_dir, record)
    parser = ctl._build_parser()[0]
    assert ctl._cmd_list(SimpleNamespace(since=None, json=True), parser) == 0
    listed = json.loads(capsys.readouterr().out)['tasks'][0]
    assert listed['responding_server'] == SELF
    assert listed['supervision']['dispatch_origin_state'] == 'dead'
    assert 'adopted_after_restart' not in listed['supervision']
    mcp_brief = store.brief(log_dir, record)
    assert mcp_brief['supervision']['adopted_after_restart']['current_server'] == SELF
