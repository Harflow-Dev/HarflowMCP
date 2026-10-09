"""Synthetic baselines must use production readers and retain bounded state semantics."""
import importlib.util
import json
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('monitor_benchmark', Path(__file__).parents[1] / 'scripts/benchmark-monitor.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_fixture_is_deterministic_and_refuses_existing_history(tmp_path):
    first, second = tmp_path / 'a', tmp_path / 'b'
    benchmark.fixture(first, 'small')
    benchmark.fixture(second, 'small')
    paths = sorted((first / '.polybridge').rglob('*.json'))
    assert paths
    for path in paths:
        assert path.read_bytes() == (second / path.relative_to(first)).read_bytes()
    with pytest.raises(ValueError, match='empty'):
        benchmark.fixture(first, 'small')


def test_large_fixture_preparation_blocked_nested_and_pagination(tmp_path):
    from polybridge import store, workflows, workflow_responses
    benchmark.fixture(tmp_path, 'large')
    root = tmp_path / '.polybridge'
    first = store.list_page(root / 'tasks')
    assert first['bootstrap_pending']
    for _ in range(30):
        page = store.list_page(root / 'tasks')
        if not page['bootstrap_pending']:
            break
    blocked = store.list_page(tmp_path / 'blocked-fixture' / '.polybridge' / 'tasks')
    assert blocked['catalog_state']['status'] == 'blocked'
    assert blocked['history_incomplete'] and page['has_more']
    second = store.list_page(root / 'tasks', cursor=page['next_cursor'])
    assert {i['task_id'] for i in page['items']}.isdisjoint(i['task_id'] for i in second['items'])
    storage = workflows.WorkflowStore(root)
    child = storage.get_run('run-0001')
    assert child['parent_link']['workflow_run_id'] == 'run-0000'
    assert child['parent_link']['execution_id'] == 'child-call'
    run = storage.get_run('run-0000')
    snapshot = workflow_responses.monitor_snapshot(run, 'run-0000', root / 'monitor_snapshots')
    assert snapshot['next_cursor']
    history = workflow_responses.history_page([run])
    assert history['runs'][0]['activations'][0]['tasks'][0]['task_id'].startswith('task-')
    for index in range(0, 400, 2):
        parent = storage.get_run(f'run-{index:04}')
        nested = storage.get_run(f'run-{index+1:04}')
        assert parent['activations'][-1]['invocation']['child_workflow_run_id'] == nested['workflow_run_id']
        assert nested['parent_link']['execution_id'] == parent['activations'][-1]['id']


def test_real_cli_is_isolated_and_reports_numeric_metrics(tmp_path):
    benchmark.fixture(tmp_path, 'small')
    response, metrics = benchmark.invoke(tmp_path, ['workflow-list-page', '--json'])
    if response.get('bootstrap_pending'):
        response, metrics = benchmark.invoke(tmp_path, ['workflow-list-page', '--json'])
    assert response['items']
    assert metrics['cli_processes'] == 1
    assert metrics['wall_ms'] > 0 and metrics['peak_rss_bytes'] > 0
    assert metrics['response_bytes'] > 0 and metrics['catalog_bootstrap_calls'] > 0
    assert all(isinstance(v, (float, int)) for v in metrics.values())
    summary, _ = benchmark.invoke(tmp_path, ['status', 'task-0000', '--json'])
    assert summary['task']['summary'] == 'Synthetic task summary 0'
    definition, _ = benchmark.invoke(tmp_path, ['workflow-get', 'bench-0000', '--json'])
    assert definition['name'] == 'bench-0000'
    blocked = benchmark.scenario(tmp_path / 'blocked-fixture', ['task-list-page', '--json'], prepare=True)
    assert blocked['complete_cli_ms'] is None
    assert blocked['first_ready_response_cli_ms'] is None
    assert blocked['terminal_cli_ms'] > 0


def test_percentiles_use_nearest_rank():
    assert benchmark.percentile([4, 1, 3, 2], .5) == 2
    assert benchmark.percentile([4, 1, 3, 2], .95) == 4


def test_fixture_worker_rejects_mutations_without_writes(tmp_path):
    benchmark.fixture(tmp_path, 'small')
    originals = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    assert benchmark.worker(['workflow-start', 'bench']) == 2
    assert originals == {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}


def test_complete_runner_uses_supported_cli_shapes(monkeypatch):
    monkeypatch.setitem(benchmark.SIZES, 'small', (2, 2, 8))
    result = benchmark.benchmark('small', 1)
    assert result['summary']['cold_workflow_open']['cli_processes_per_repeat'][0] >= 3
    assert result['summary']['warm_reopen']['cli_processes_per_repeat'] == [1]
    assert result['summary']['saved_workflow_open']['n'] == 1
    assert next(row for row in result['raw'] if row['scenario'] == 'blocked_history')['complete_cli_ms'] is None
