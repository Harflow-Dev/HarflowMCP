"""Self-checks for the E1 read scenarios: seeding fidelity and sensitivity to a deviating engine."""
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

from conformance.e1 import cases, scenarios, seed
from conformance.oracles import strict_json

REAL = [sys.executable, '-I', '-B', '-m', 'harflow_mcp.cli']
PROXY = Path(__file__).with_name('proxy_engine.py')


def run_selected(tmp_path, mode, names):
    command = [sys.executable, '-I', '-B', str(PROXY), mode, *REAL, '--']
    return {row['name']: row for row in scenarios.run_cases(command, {}, lambda: None, tmp_path / 'out', selected=names)}


def test_seed_marker_matches_the_documented_store_marker(tmp_path):
    tasks = seed.init_root(tmp_path / 'root')
    marker = strict_json((tmp_path / 'root' / '.harflow-store.json').read_bytes())
    assert marker == {'format': 'harflow.store', 'layout_version': 1, 'product': 'harflow', 'product_version': '0.0.11', 'v': 1}
    assert sorted(p.name for p in (tmp_path / 'root').iterdir()) == [
        '.harflow-store.json', '.harflow-store.lock', 'tasks', 'workflow-leases', 'workflow-owners', 'workflow-runs', 'workflows']
    assert tasks == tmp_path / 'root' / 'tasks'
    assert (tmp_path / 'root' / '.harflow-store.lock').read_bytes() == b''


def test_seeded_documents_have_the_documented_private_envelopes(tmp_path):
    tasks = seed.seed_reads(tmp_path / 'root')
    for task_id in (seed.A, seed.B, seed.C, seed.D, seed.E):
        document = strict_json((tasks / f'{task_id}.meta.json').read_bytes())
        assert (document['product'], document['format'], document['v']) == ('harflow', 'harflow.task', 1)
        assert document['task_id'] == task_id
    for task_id in (seed.A, seed.B):
        lines = (tasks / f'{task_id}.events.jsonl').read_text().splitlines()
        events = [strict_json(line) for line in lines]
        assert [e['seq'] for e in events] == list(range(len(events)))
        assert all((e['product'], e['format'], e['v']) == ('harflow', 'harflow.event', 1) and e['task_id'] == task_id for e in events)
    sidecar = tasks / f'{seed.D}.prompt.{hashlib.sha256(seed.LONG_PROMPT.encode()).hexdigest()}.txt'
    assert sidecar.read_text() == seed.LONG_PROMPT
    assert len(seed.LONG_PROMPT) > 2000 and len(seed.LONG_PROMPT.encode()) > len(seed.LONG_PROMPT)
    assert oct(os.stat(tasks / f'{seed.A}.meta.json').st_mode & 0o777) == '0o600'


def test_seed_refuses_to_overwrite(tmp_path):
    tasks = seed.seed_reads(tmp_path / 'root')
    with pytest.raises(FileExistsError):
        seed.write_task(tasks, seed.A)


def test_every_case_is_registered_with_a_capability_list():
    assert len(cases.CASES) >= 29
    for name, (capabilities, function) in cases.CASES.items():
        assert name.count('.') == 1 and callable(function) and 'ctl.v1' in capabilities


def test_scenarios_reject_an_engine_that_answers_nothing_useful(tmp_path):
    results = run_selected(tmp_path, 'empty', ['list.populated', 'status.exact_and_prefix', 'task-prompt.errors'])
    assert set(results) == {'list.populated', 'status.exact_and_prefix', 'task-prompt.errors'}
    assert all(not row['ok'] and row['failures'] for row in results.values())


def test_scenarios_detect_specific_deviations(tmp_path):
    names = ['status.exact_and_prefix', 'list.populated']
    assert all(row['ok'] for row in run_selected(tmp_path / 'a', 'passthrough', names).values())
    dropped = run_selected(tmp_path / 'b', 'drop-resolved-from', names)
    assert not dropped['status.exact_and_prefix']['ok'] and dropped['list.populated']['ok']
    assert any('resolved_from' in message or 'prefix' in message for message in dropped['status.exact_and_prefix']['failures'])
    wrong = run_selected(tmp_path / 'c', 'wrong-status', names)
    assert not wrong['status.exact_and_prefix']['ok'] and wrong['list.populated']['ok']


def test_scenarios_detect_a_missing_authority_refusal_and_a_dropped_event(tmp_path):
    refusal = run_selected(tmp_path / 'a', 'no-authority', ['scope.agent_caller_by_ancestry'])
    assert not refusal['scope.agent_caller_by_ancestry']['ok']
    events = run_selected(tmp_path / 'b', 'mutate-events', ['task-events.kind_filters'])
    assert not events['task-events.kind_filters']['ok']


def test_a_case_that_raises_is_a_failure(tmp_path, monkeypatch):
    def broken(ctx):
        raise RuntimeError('boom')
    monkeypatch.setitem(cases.CASES, 'x.boom', (('ctl.v1',), broken))
    result = run_selected(tmp_path, 'passthrough', ['x.boom'])
    assert not result['x.boom']['ok'] and 'boom' in result['x.boom']['failures'][0]


def test_a_read_that_rewrites_authoritative_state_is_a_failure(tmp_path):
    result = run_selected(tmp_path, 'mutate-state', ['list.populated'])
    assert not result['list.populated']['ok']
    assert any('authoritative documents unchanged' in message for message in result['list.populated']['failures'])
