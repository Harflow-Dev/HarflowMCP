"""Parallel baselines are deterministic and isolated, with actual normalized event logs."""
import importlib.util
import json
from pathlib import Path
import pytest

spec = importlib.util.spec_from_file_location('parallel_benchmark', Path(__file__).parents[1] / 'scripts/benchmark-parallel.py')
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_fixture_members_resume_chains_and_normalized_events(tmp_path):
    manifest = benchmark.fixture(tmp_path, 8, 16, 'sparse')
    tasks = tmp_path / '.polybridge/tasks'
    assert manifest['tasks'] == 128
    assert len(list(tasks.glob('*.meta.json'))) == 128
    for conversation in range(8):
        session = set()
        for turn in range(16):
            task_id = f'parallel-{conversation:04}-{turn:02}'
            meta = json.loads((tasks / f'{task_id}.meta.json').read_text())
            assert meta['group'] == benchmark.GROUP
            assert meta['parent_task_id'] == (f'parallel-{conversation:04}-{turn-1:02}' if turn else None)
            session.add(meta['session_id'])
            rows = [json.loads(line) for line in (tasks / f'{task_id}.events.jsonl').read_text().splitlines()]
            assert all(row['v'] == 1 and row['task_id'] == task_id for row in rows)
            assert [row['seq'] for row in rows] == list(range(len(rows)))
            assert [row['kind'] for row in rows] == ['task_started','user_message','tool_call','tool_result','tool_call','tool_result','assistant_text','task_finished']
        assert len(session) == 1


def test_fixture_refuses_any_nonempty_home(tmp_path):
    sentinel = tmp_path / 'personal.txt'
    sentinel.write_text('untouched')
    with pytest.raises(ValueError, match='empty'):
        benchmark.fixture(tmp_path)
    assert sentinel.read_text() == 'untouched'
    assert not (tmp_path / '.polybridge').exists()


def test_deterministic_long_logs_and_repeat_bursts(tmp_path):
    first, second = tmp_path / 'first', tmp_path / 'second'
    benchmark.fixture(first, 8, 1, 'long')
    benchmark.fixture(second, 8, 1, 'long')
    for path in (first / '.polybridge/tasks').iterdir():
        assert path.read_bytes() == (second / '.polybridge/tasks' / path.name).read_bytes()
    path = first / '.polybridge/tasks/parallel-0000-00.events.jsonl'
    original = path.read_bytes()
    assert len(original.splitlines()) == 516
    assert benchmark.append_burst(first, 3) == {'tasks_appended':8,'events_appended':24}
    benchmark.append_burst(first, 2)
    assert path.read_bytes().startswith(original)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    assert [row['seq'] for row in rows] == list(range(521))


def test_bursts_only_current_checkpoints(tmp_path):
    benchmark.fixture(tmp_path, 8, 16)
    paths = list((tmp_path / '.polybridge/tasks').glob('*.events.jsonl'))
    before = {path:path.read_bytes() for path in paths}
    benchmark.append_burst(tmp_path, 1)
    for path in paths:
        assert (path.read_bytes() != before[path]) == path.name.endswith('-15.events.jsonl')


def test_unsupported_shape_and_missing_marker_refused(tmp_path):
    with pytest.raises(ValueError): benchmark.fixture(tmp_path, 9)
    with pytest.raises(FileNotFoundError): benchmark.append_burst(tmp_path)
    assert len(benchmark.protocol()['shapes']) == 12


def test_append_refuses_symlink_without_external_changes(tmp_path):
    home, other = tmp_path / 'home', tmp_path / 'other'
    benchmark.fixture(home)
    other.write_text('outside')
    target = home / '.polybridge/tasks/parallel-0000-00.events.jsonl'
    target.unlink()
    target.symlink_to(other)
    with pytest.raises(ValueError, match='local'):
        benchmark.append_burst(home)
    assert other.read_text() == 'outside'


def test_real_cli_reports_parallel_group_and_resume_headers(tmp_path):
    benchmark.fixture(tmp_path, 8, 16)
    monitor = benchmark.monitor_module()
    for _ in range(30):
        page, _ = monitor.invoke(tmp_path, ['task-list-page', '--json'])
        if not page.get('bootstrap_pending'): break
    items = page['items']
    while page.get('next_cursor'):
        page, _ = monitor.invoke(tmp_path, ['task-list-page', '--json', '--cursor', page['next_cursor']])
        items += page['items']
    assert len(items) == 128
    assert {item['group'] for item in items} == {benchmark.GROUP}
    assert sum(item['parent_task_id'] is not None for item in items) == 120
    summary, _ = monitor.invoke(tmp_path, ['status', 'parallel-0000-15', '--json'])
    assert summary['task']['summary'] == 'Synthetic benchmark answer'


def test_workflow_associates_every_checkpoint_with_selected_node(tmp_path):
    from polybridge import workflows, workflow_responses
    benchmark.fixture(tmp_path, 8, 16, workflow=True)
    storage = workflows.WorkflowStore(tmp_path / '.polybridge')
    run = storage.get_run('parallel-run')
    assert run['name'] == 'Synthetic parallel workflow'
    assert run['activations'][0]['node_id'] == 'work'
    associations = run['activations'][0]['tasks']
    assert len(associations) == 128
    assert {entry['task_id'] for entry in associations} == {path.name.removesuffix('.meta.json') for path in (tmp_path / '.polybridge/tasks').glob('*.meta.json')}
    snapshot = workflow_responses.monitor_snapshot(run, 'parallel-run', tmp_path / '.polybridge/monitor_snapshots')
    assert snapshot['monitor_snapshot']
    marker = json.loads((tmp_path / 'monitor-benchmark-fixture.json').read_text())
    assert marker['definitions'] == marker['runs'] == 1
