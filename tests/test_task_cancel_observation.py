"""Ctl-only cancellation evidence, races, failure isolation and cooperative bounds."""
import asyncio
from copy import deepcopy
from dataclasses import replace
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp import control, ctl, identity, store, task_resources, task_supervision
from harflow_mcp import task_cancel_observation as observation
from harflow_mcp import observation_budget
from harflow_mcp.tasks import TaskRegistry

ROOT = Path(__file__).parents[1]


def generator():
    spec = importlib.util.spec_from_file_location('cancel_fixtures', ROOT / 'scripts/generate-cancel-cascade-fixtures.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def source(tmp_path, monkeypatch):
    logs = tmp_path / 'state' / 'tasks'
    gen = generator()
    gen.record(logs, gen.TARGET)
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: logs)
    monkeypatch.setattr(identity, 'own_identity', lambda: gen.OWNER)
    monkeypatch.setattr(identity, 'identity_check', lambda value: 'dead')
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('dead', 'start_time_differs'))
    monkeypatch.setattr(store, 'process_alive', lambda *a: False)
    return logs, gen


def observe(source, **changes):
    logs, gen = source
    if changes:
        record = store.read(logs, gen.TARGET)
        assert store.write_landed(logs, replace(record, **changes))
    return observation.Collector(gen.TARGET).project(logs)


@pytest.mark.parametrize('exit_code', [None, 0, -15])
@pytest.mark.parametrize('drained', [False, True])
@pytest.mark.parametrize('verdict', ['dead', 'alive', 'undecidable'])
def test_recorded_matrix(source, monkeypatch, exit_code, drained, verdict):
    monkeypatch.setattr(identity, 'identity_check', lambda value: verdict)
    monkeypatch.setattr(identity, 'check_detail', lambda value: (verdict, 'controlled'))
    result = observe(source, exit_code=exit_code, output_drained=drained)
    row = result['items'][0]
    expected = 'running' if verdict == 'alive' else 'stopped' if verdict == 'dead' and exit_code is not None and drained is True else 'unknown'
    assert row['state'] == expected
    assert result['coverage'] == 'complete'
    assert row['status'] is not None


@pytest.mark.parametrize('change', [dict(pid=None), dict(start_time=None), dict(pid=None, start_time=None)])
def test_missing_captured_identity_is_not_death(source, change):
    assert observe(source, **change)['items'][0]['state'] == 'unknown'


def test_probe_exception_is_per_target_uncertainty(source, monkeypatch):
    def broken(value):
        raise OSError('probe unavailable')
    monkeypatch.setattr(identity, 'identity_check', broken)
    result = observe(source)
    assert result['coverage'] == 'incomplete'
    assert result['items'][0] == dict(task_id=source[1].TARGET, state='unknown', basis='insufficient_evidence', status='completed')


@pytest.mark.parametrize('race', ['resolve-replacement', 'probe-replacement', 'probe-disappearance', 'incompatible'])
def test_status_and_settlement_use_same_qualified_source(source, monkeypatch, race):
    logs, gen = source
    read = store.read
    count = 0
    def racing_read(directory, task_id, **kwargs):
        nonlocal count
        count += 1
        if race == 'resolve-replacement' and count == 2:
            gen.record(logs, task_id, pid=6001)
        return read(directory, task_id, **kwargs)
    monkeypatch.setattr(store, 'read', racing_read)
    def probe(value):
        if race == 'probe-replacement':
            gen.record(logs, gen.TARGET, exit_code=1)
        elif race == 'probe-disappearance':
            store.record_path(logs, gen.TARGET).unlink()
        elif race == 'incompatible':
            store.record_path(logs, gen.TARGET).write_text('{}')
        return 'dead'
    monkeypatch.setattr(identity, 'identity_check', probe)
    result = observe(source)
    assert result['coverage'] == 'incomplete'
    assert result['items'][0]['state'] == 'unknown'
    assert result['items'][0]['status'] is None


def test_no_index_or_reconstructed_status_proves_settlement(source):
    result = observe(source, exit_code=None, status='cancelled')
    assert result['coverage'] == 'complete'
    assert result['items'][0]['state'] == 'unknown'
    assert result['items'][0]['status'] == 'cancelled'


def test_candidate_cap_precedes_all_reads_and_probes(monkeypatch):
    collector = observation.Collector('target')
    collector.discover([f'child-{i:03}' for i in range(100)])
    reads, probes = [], []
    def read(logs, task_id, **kwargs):
        reads.append(task_id)
        return SimpleNamespace(task_id=task_id, pid=1, start_time='captured', markers=[], exit_code=0,
                               output_drained=True, _source_identity=('source', task_id))
    def resolve(logs, record, **kwargs):
        kwargs['_observer_record'].append(record)
        return 'completed', '', None, []
    monkeypatch.setattr(task_supervision, 'read_task_record', read)
    monkeypatch.setattr(store, 'resolve_status', resolve)
    monkeypatch.setattr(identity, 'identity_check', lambda value: probes.append(value) or 'dead')
    result = collector.project(Path('/unused'))
    ids = [row['task_id'] for row in result['items']]
    assert ids == ['target', *[f'child-{i:03}' for i in range(99)]]
    assert len(probes) == 100 and len(reads) == 200
    assert 'child-099' not in reads
    assert result['truncated'] and result['coverage'] == 'incomplete'


@pytest.mark.parametrize('limit', [0, 200, 20000])
def test_one_shared_read_allowance(source, monkeypatch, limit):
    logs, gen = source
    gen.record(logs, gen.CHILD, spawned_by=gen.TARGET)
    budget = observation_budget.Budget(byte_limit=limit)
    monkeypatch.setattr(observation, 'Budget', lambda: budget)
    collector = observation.Collector(gen.TARGET)
    collector.discover([gen.CHILD])
    result = collector.project(logs)
    assert budget.metadata_bytes <= limit
    if limit < 20000:
        assert result['coverage'] == 'incomplete'
        assert all(row['status'] is None for row in result['items'])
    assert result['truncated'] is False
    assert observation_budget.current.get() is None


def test_shared_deadline_does_not_start_remaining_probes(source, monkeypatch):
    logs, gen = source
    gen.record(logs, gen.CHILD, spawned_by=gen.TARGET)
    budget = observation_budget.Budget()
    monkeypatch.setattr(observation, 'Budget', lambda: budget)
    probes = []
    def probe(value):
        probes.append(value)
        budget.deadline = 0
        return 'dead'
    monkeypatch.setattr(identity, 'identity_check', probe)
    collector = observation.Collector(gen.TARGET)
    collector.discover([gen.CHILD])
    result = collector.project(logs)
    assert len(probes) == 1
    assert len(result['items']) == 2 and result['coverage'] == 'incomplete'
    assert all(row['status'] is None for row in result['items'])


def test_identity_subprocess_timeout_uses_remaining_budget(monkeypatch):
    calls = []
    monkeypatch.setattr(identity.subprocess, 'run', lambda *a, **kw: calls.append(kw) or SimpleNamespace(returncode=1, stdout='', stderr=''))
    token = observation_budget.current.set(observation_budget.Budget(seconds=0.5))
    try:
        assert identity.identity_check(dict(pid=1, start_time='capture', markers=[])) == 'dead'
    finally:
        observation_budget.current.reset(token)
    assert 0 < calls[0]['timeout'] <= 0.5


def test_partial_log_replay_is_not_a_resolved_status(source, monkeypatch):
    logs, gen = source
    record = store.read(logs, gen.TARGET)
    store.write(logs, replace(record, exit_code=None))
    store.log_path(logs, gen.TARGET).write_bytes(b'{}\n' + b'x' * 100000)
    budget = observation_budget.Budget(byte_limit=25000)
    monkeypatch.setattr(observation, 'Budget', lambda: budget)
    result = observe(source)
    assert result['items'][0]['status'] is None
    assert result['coverage'] == 'incomplete'
    assert budget.metadata_bytes <= 25000


def test_observer_status_resolution_never_recovers_or_writes_control_phases(source, monkeypatch):
    logs, gen = source
    gen.record(logs, gen.TARGET, status='cancelled', exit_code=None)
    control.write_phase(logs, gen.TARGET, control.CANCEL, 1, 'req',
        dict(at='2001-01-01T00:00:00+00:00', by=gen.OWNER, lease_seconds=1))
    def forbidden(*args):
        pytest.fail('Observer must not recover an abandoned controller')
    monkeypatch.setattr(control, 'recover_abandoned', forbidden)
    result = observe(source)
    assert result['items'][0]['status'] == 'cancelled'
    assert not control.phase_path(logs, gen.TARGET, control.CANCEL, 1, 'failed').exists()


def test_phase_read_exhaustion_cannot_be_swallowed_into_resolved_status(source, monkeypatch):
    logs, gen = source
    gen.record(logs, gen.TARGET, status='cancelled', exit_code=None)
    control.write_phase(logs, gen.TARGET, control.CANCEL, 1, 'sig', dict(at=gen.STAMP, leader_alive=True))
    path = control.phase_path(logs, gen.TARGET, control.CANCEL, 1, 'sig')
    path.write_bytes(path.read_bytes() + b' ' * 100000)
    budget = observation_budget.Budget(byte_limit=25000)
    monkeypatch.setattr(observation, 'Budget', lambda: budget)
    result = observe(source)
    assert result['coverage'] == 'incomplete'
    assert result['items'][0]['status'] is None
    assert budget.exhausted and budget.metadata_bytes <= 25000


@pytest.mark.parametrize('kind', ['fifo', 'directory', 'symlink'])
def test_observer_replay_refuses_nonregular_sources_without_blocking(source, kind):
    logs, gen = source
    gen.record(logs, gen.TARGET, status='cancelled', exit_code=None)
    path = store.log_path(logs, gen.TARGET)
    if kind == 'fifo':
        os.mkfifo(path)
    elif kind == 'directory':
        path.mkdir()
    else:
        target = logs / 'other-stream'
        target.write_text('{}\n')
        path.symlink_to(target)
    result = observe(source)
    assert result['coverage'] == 'incomplete' and result['items'][0]['status'] is None


def test_serialized_byte_boundary_unicode_and_whole_rows(monkeypatch):
    original = observation.fallback
    monkeypatch.setattr(observation, 'fallback', lambda: original() | dict(observed_at='2026-10-15T00:00:00+00:00'))
    collector = observation.Collector('target')
    for task_id in ['target', 'escaped-"-é-界']:
        collector.receipt(task_id, dict(task_id=task_id, state='waiting_for_resources'))
    monkeypatch.setattr(task_supervision, 'read_task_record', lambda *a, **kw: None)
    monkeypatch.setattr(task_supervision, 'read_launch', lambda logs, task_id: collector.pending[task_id] | dict(state='waiting_for_resources'))
    full = collector.project(Path('/unused'))
    size = len(json.dumps(full).encode('utf-8'))
    monkeypatch.setattr(observation, 'MAX_BYTES', size)
    assert collector.project(Path('/unused')) == full
    monkeypatch.setattr(observation, 'MAX_BYTES', size - 1)
    bounded = collector.project(Path('/unused'))
    assert bounded['items'] == full['items'][:1]
    assert bounded['truncated'] and bounded['coverage'] == 'incomplete'
    assert len(json.dumps(bounded).encode('utf-8')) <= size - 1


@pytest.mark.parametrize('fault', ['discover', 'processed', 'evidence', 'project'])
def test_private_faults_do_not_replace_attempted_cancellation(source, monkeypatch, capsys, fault):
    logs, gen = source
    gen.record(logs, gen.TARGET, auto_continue=1)
    from harflow_mcp.control import PhaseWriteError
    def broken(*args):
        raise PhaseWriteError('private observer fault')
    monkeypatch.setattr(observation.Collector, fault, broken)
    assert ctl.main(['cancel', gen.TARGET, '--json']) == 0
    doc = json.loads(capsys.readouterr().out)
    assert doc['result']['cascade']['observation']['coverage'] == 'unknown'
    assert (logs / f'{gen.TARGET}.continuation-stopped.json').exists()
    assert 'error' not in doc


def test_nonserializable_projection_uses_exact_minimal_fallback(source, monkeypatch):
    collector = observation.Collector(source[1].TARGET)
    monkeypatch.setattr(collector, 'project', lambda logs: {'invalid': object()})
    result = observation.finish(collector, source[0])
    assert set(result) == {'scope', 'observed_at', 'coverage', 'truncated', 'items'}
    assert result['coverage'] == 'unknown' and result['items'] == [] and not result['truncated']


@pytest.mark.parametrize('fault', ['receipt', 'settlement'])
def test_pending_collection_faults_preserve_real_settlement(source, monkeypatch, capsys, fault):
    logs, gen = source
    gen.receipt(logs, gen.CHILD)
    def broken(*args):
        raise ValueError('private pending collection failed')
    monkeypatch.setattr(observation.Collector, fault, broken)
    assert ctl.main(['cancel', gen.CHILD, '--json']) == 0
    result = json.loads(capsys.readouterr().out)['result']
    assert result['cascade']['pending_settled'] == [gen.CHILD]
    assert result['cascade']['observation']['coverage'] == 'unknown'
    assert task_supervision.read_launch(logs, gen.CHILD)['state'] == 'not_started'


def test_constructor_failure_preserves_cancel_result(source, monkeypatch, capsys):
    def broken(*args):
        raise ValueError('observer could not be built')
    monkeypatch.setattr(observation, 'Collector', broken)
    assert ctl.main(['cancel', source[1].TARGET, '--json']) == 0
    assert json.loads(capsys.readouterr().out)['result']['cascade']['observation']['coverage'] == 'unknown'


def test_registry_and_mcp_legacy_cascade_shape_has_no_private_evidence(source):
    logs, gen = source
    result = asyncio.run(TaskRegistry(log_dir=logs).cancel_cascade(gen.TARGET))
    assert 'observation' not in result
    assert not any(key.startswith('_') for key in result)
    assert set(result) == {'cancelled_descendants', 'sigkill_survivors', 'owner_still_settling',
                          'not_signalled', 'not_recorded', 'rounds', 'cascade_incomplete',
                          'history_incomplete', 'refused_tasks', 'unconverged'}


def test_pending_requires_own_settlement_and_binding(source):
    logs, gen = source
    launch = gen.receipt(logs, gen.CHILD, state='not_started', not_started_reason='cancelled')
    collector = observation.Collector(gen.CHILD)
    binding = collector.receipt(gen.CHILD, launch)
    assert collector.project(logs)['items'][0]['state'] == 'unknown'
    collector.settlement(gen.CHILD, binding)
    assert collector.project(logs)['items'][0]['basis'] == 'proven_not_started'
    task_supervision.mark(logs, gen.CHILD, launch['generation'], request_sha256='f' * 64)
    result = collector.project(logs)
    assert result['coverage'] == 'incomplete' and result['items'][0]['state'] == 'unknown'


@pytest.mark.parametrize('settle_before_replacement', [False, True])
def test_pending_settlement_cannot_transfer_to_replacement_generation(source, settle_before_replacement):
    logs, gen = source
    collector = observation.Collector(gen.CHILD)
    launch_a = gen.receipt(logs, gen.CHILD, state='not_started', not_started_reason='cancelled')
    binding_a = collector.receipt(gen.CHILD, launch_a)
    if settle_before_replacement:
        collector.settlement(gen.CHILD, binding_a)
        assert collector.project(logs)['items'][0]['basis'] == 'proven_not_started'
    launch_b = gen.receipt(logs, gen.CHILD, generation='f' * 32, state='not_started', not_started_reason='cancelled')
    binding_b = collector.receipt(gen.CHILD, launch_b)
    collector.settlement(gen.CHILD, binding_a)
    result = collector.project(logs)
    assert result['items'][0]['state'] == 'unknown'
    assert result['coverage'] == 'incomplete'
    collector.settlement(gen.CHILD, binding_b)
    assert collector.project(logs)['items'][0]['basis'] == 'proven_not_started'
    collector.settlement(gen.CHILD, binding_a)
    assert collector.project(logs)['items'][0]['basis'] == 'proven_not_started'


@pytest.mark.parametrize('field, replacement', [
    ('generation', 'f' * 32), ('request_sha256', 'f' * 64), ('configuration_sha256', 'f' * 64),
    ('root', '/other-root'), ('directory_binding', dict(path='/other-root/tasks', inode=123)),
    ('directory_identity', [123, 456]), ('owner', dict(pid=6001, start_time='other-owner', markers=[])),
])
def test_pending_settlement_binding_captures_all_receipt_identity_fields(source, field, replacement):
    logs, gen = source
    collector = observation.Collector(gen.CHILD)
    launch = gen.receipt(logs, gen.CHILD)
    binding_a = collector.receipt(gen.CHILD, launch)
    collector.settlement(gen.CHILD, binding_a)
    changed = deepcopy(launch)
    changed[field] = replacement
    binding_b = collector.receipt(gen.CHILD, changed)
    assert gen.CHILD not in collector.settled
    collector.settlement(gen.CHILD, binding_a)
    assert gen.CHILD not in collector.settled
    collector.settlement(gen.CHILD, binding_b)
    assert collector.settled[gen.CHILD] == binding_b


def test_pending_receipt_binding_is_a_snapshot(source):
    logs, gen = source
    launch = gen.receipt(logs, gen.CHILD)
    collector = observation.Collector(gen.CHILD)
    binding = collector.receipt(gen.CHILD, launch)
    expected = deepcopy(binding)
    launch['owner']['pid'] = 9999
    launch['directory_binding']['inode'] = 9999
    assert collector.pending[gen.CHILD] == expected
    collector.settlement(gen.CHILD, binding)
    binding['owner']['pid'] = 8888
    assert collector.settled[gen.CHILD] == expected


def test_cancel_launch_contributes_its_own_captured_settlement_binding(source, monkeypatch):
    logs, gen = source
    collector = observation.Collector(gen.CHILD)
    gen.receipt(logs, gen.CHILD)
    def settle_a(directory, launch_a):
        assert launch_a['generation'] == 'c' * 32
        launch_b = gen.receipt(logs, gen.CHILD, generation='f' * 32,
                               state='not_started', not_started_reason='cancelled')
        collector.receipt(gen.CHILD, launch_b)
        return True
    monkeypatch.setattr(task_resources, 'settle_cancelled', settle_a)
    result = task_resources.cancel_launch(logs, gen.CHILD, authorized=True, _observation=collector)
    assert result['cascade']['pending_settled'] == [gen.CHILD]
    assert collector.project(logs)['items'][0]['state'] == 'unknown'


@pytest.mark.parametrize('reader', ['read', 'resolve_status'])
@pytest.mark.parametrize('fault', ['malformed', 'incompatible', 'read_refusal', 'read_limit', 'read_exhaustion', 'deadline'])
def test_observer_read_failures_never_reconcile_catalog(source, monkeypatch, reader, fault):
    from harflow_mcp import bounded_io, catalog
    logs, gen = source
    record = store.read(logs, gen.TARGET)
    budget = observation_budget.Budget()
    expected = store.TaskDocumentError
    if fault in ('malformed', 'incompatible'):
        store.record_path(logs, gen.TARGET).write_text('{' if fault == 'malformed' else '{}')
    elif fault in ('read_refusal', 'read_limit'):
        error = OSError('metadata refused') if fault == 'read_refusal' else bounded_io.ReadLimit('read limit')
        expected = store.TaskDocumentError if fault == 'read_refusal' else bounded_io.ReadLimit
        def refused(*args, **kwargs):
            raise error
        monkeypatch.setattr(bounded_io, 'read_json', refused)
    else:
        expected = observation_budget.Exhausted
        if fault == 'deadline':
            budget.deadline = 0
        else:
            budget.metadata_limit = 0
    def forbidden(*args, **kwargs):
        pytest.fail('Observer refusal must not publish or lock a derivative catalog')
    monkeypatch.setattr(catalog, 'Catalog', forbidden)
    monkeypatch.setattr(store, '_blocker', forbidden)
    token = observation_budget.current.set(budget)
    try:
        with pytest.raises(expected):
            if reader == 'read':
                store.read(logs, gen.TARGET, include_prompt=False)
            else:
                store.resolve_status(logs, record, detail=False)
    finally:
        observation_budget.current.reset(token)


def test_default_read_refusal_still_reconciles_catalog(source, monkeypatch):
    from harflow_mcp import catalog
    logs, gen = source
    store.record_path(logs, gen.TARGET).write_text('{}')
    published = []
    monkeypatch.setattr(catalog.Catalog, 'record', lambda *args, **kwargs: published.append((args, kwargs)))
    with pytest.raises(store.TaskDocumentError):
        store.read(logs, gen.TARGET, include_prompt=False)
    assert len(published) == 1


@pytest.mark.parametrize('first_ending, final_ending', [('\n', '\n'), ('\r', '\r'), ('\r\n', '\r\n'), ('\r\n', '')])
def test_bounded_replay_preserves_json_unicode_separators_and_universal_newlines(source, first_ending, final_ending):
    logs, gen = source
    gen.record(logs, gen.TARGET, status='failed', exit_code=None)
    summary = 'before\u2028middle\u2029after'
    init = dict(type='system', subtype='init', session_id='fixture-session')
    result = dict(type='result', subtype='success', result=summary, is_error=False)
    raw = json.dumps(init, ensure_ascii=False) + first_ending + json.dumps(result, ensure_ascii=False) + final_ending
    store.log_path(logs, gen.TARGET).write_bytes(raw.encode('utf-8'))
    record = store.read(logs, gen.TARGET)
    ordinary = store.resolve_status(logs, record, detail=False)
    token = observation_budget.current.set(observation_budget.Budget())
    try:
        bounded = store.resolve_status(logs, record, detail=False)
    finally:
        observation_budget.current.reset(token)
    assert ordinary[0] == 'completed' and ordinary[2].summary == summary
    assert bounded == ordinary


def test_pending_to_recorded_ctl_recursion_preserves_prior_evidence(source, monkeypatch, capsys):
    logs, gen = source
    store.record_path(logs, gen.TARGET).unlink()
    gen.receipt(logs, gen.TARGET)
    original = task_resources.cancel_launch
    def spawned(directory, task_id, **kwargs):
        result = original(directory, task_id, **kwargs)
        collector = kwargs['_observation']
        gen.receipt(logs, gen.CHILD)
        collector.discover([gen.CHILD])
        collector.evidence(dict(withdrawal_unsettled=[gen.CHILD], launch_refusals=[dict(task_id='unqualified')]))
        gen.record(logs, gen.TARGET)
        return result | {'spawned': True}
    monkeypatch.setattr(task_resources, 'cancel_launch', spawned)
    assert ctl.main(['cancel', gen.TARGET, '--json']) == 0
    result = json.loads(capsys.readouterr().out)['result']['cascade']['observation']
    assert [row['task_id'] for row in result['items']] == [gen.TARGET, gen.CHILD]
    assert result['coverage'] == 'incomplete'


def test_cumulative_flags_survive_legacy_clean_merge():
    collector = observation.Collector('target')
    first = dict(unconverged=['new'], refused_tasks=[], cascade_incomplete=True, rounds=2)
    collector.evidence(first)
    latest = dict(unconverged=[], refused_tasks=[], cascade_incomplete=False, rounds=1)
    merged = TaskRegistry._merge_cascade_results(first, latest)
    collector.evidence(merged)
    assert merged['unconverged'] == [] and merged['cascade_incomplete'] is False
    assert merged['rounds'] == 2 and collector.incomplete


def test_contract_fixtures_reproduce_twice_and_exact_shape():
    gen = generator()
    first = gen.generate()
    assert first == gen.generate()
    for path, raw in first.items():
        assert path.read_bytes() == raw, path
        if not path.name.startswith('ctl-task-cancel-'):
            continue
        observation_doc = json.loads(raw)['result']['cascade']['observation']
        assert set(observation_doc) == {'scope', 'observed_at', 'coverage', 'truncated', 'items'}
        assert len(json.dumps(observation_doc).encode()) <= 65536
        for row in observation_doc['items']:
            assert set(row) == {'task_id', 'state', 'basis', 'status'}
        assert len(observation_doc['items']) <= 100
    refused = json.loads(first[gen.BUNDLE / 'ctl-task-cancel-refused-documents.json'])['result']['cascade']
    assert refused['refused_tasks'] and refused['launch_refusals']
    assert len(refused['observation']['items']) == 1
    unsettled = json.loads(first[gen.BUNDLE / 'ctl-task-cancel-pending-child-withdrawal-unsettled.json'])['result']['cascade']
    assert unsettled['cascade_incomplete'] is False
    assert unsettled['observation']['coverage'] == 'incomplete'
    assert unsettled['observation']['items'][1]['state'] == 'unknown'
