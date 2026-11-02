"""Native notification feed regression tests: no outbound deliveries or agent processes."""
import json
import time
from dataclasses import replace

import pytest

from harflow_mcp import approvals, interaction_events as feed, store, workflows as w
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from test_caller_approvals import _admit
from test_workflow_notification_points import definition


@pytest.fixture
def logs(tmp_path):
    path = tmp_path / 'state' / 'tasks'
    with state_session(resolve_layout(root=path.parent)):
        path.mkdir()
        yield path


def task(logs, identifier='task-1', status='running', exit_code=None):
    record = store.TaskRecord(task_id=identifier, backend='codex', session_id=None,
                              repo_path='/project', started_at='2026-11-01T00:00:00+00:00',
                              status=status, exit_code=exit_code, title='Build', prompt='SECRET')
    store.write(logs, record)
    return record


def cycle(logs, *, limit=100):
    cursor = None
    found = []
    for _ in range(100):
        result = feed.page(logs, cursor, limit)
        found.extend(result['events'])
        if result['cycle_complete']:
            return found
        cursor = result['next_cursor']
    raise AssertionError('cycle did not terminate')


def test_empty_root_is_read_only(tmp_path):
    root = tmp_path / 'absent'
    assert feed.page(root / 'tasks')['cycle_complete']
    assert not root.exists()
    root.mkdir()
    assert feed.page(root / 'tasks')['events'] == []
    assert list(root.iterdir()) == []


def test_answered_approval_replays_both_events_with_winning_provenance(logs):
    task(logs)
    row = _admit(logs)
    approvals.reserve(logs, row['task_id'], row['request_id'], row['action_digest'], 1, 'approve', row['generation'], responder='monitor')
    with pytest.raises(approvals.ApprovalError):
        approvals.reserve(logs, row['task_id'], row['request_id'], row['action_digest'], 1, 'reject', row['generation'])
    events = cycle(logs, limit=1)
    assert [event['state'] for event in events] == ['pending', 'answered']
    assert events[1]['answered_by'] == 'monitor'
    assert events[1]['answer'] == 'approve'
    assert len({event['event_id'] for event in events}) == 2
    assert cycle(logs, limit=1) == events
    serialized = json.dumps(events)
    assert 'printf' not in serialized and 'bridge_owner' not in serialized and 'SECRET' not in serialized


def test_terminal_tasks_retained_after_restart_and_unobserved_failure_excluded(logs):
    task(logs, 'done', 'completed', 0)
    task(logs, 'failed', 'failed', 1)
    task(logs, 'guessed', 'failed', None)
    events = cycle(logs, limit=1)
    assert {(event['task_id'], event['state']) for event in events} == {('done', 'completed'), ('failed', 'failed')}
    assert all(isinstance(event['occurred_at'], float) for event in events)


def test_paginated_scan_reaches_late_source_and_next_cycle_sees_earlier_write(logs):
    for i in range(30):
        task(logs, f'task-{i:03}')
    cursor = feed.page(logs)['next_cursor']
    assert cursor
    task(logs, 'task-000', 'completed', 0)
    task(logs, 'task-029', 'completed', 0)
    later = feed.page(logs, cursor)
    assert any(event['task_id'] == 'task-029' for event in later['events'])
    assert {event['task_id'] for event in cycle(logs)} == {'task-000', 'task-029'}


def test_workflow_pending_then_answer_terminal_between_polls(logs, monkeypatch):
    monkeypatch.setattr(w.backends, 'is_installed', lambda _: True)
    storage = w.WorkflowStore(logs.parent)
    run = storage.create_run(w.validate_definition(definition()), 'Goal', logs.parent)
    identifier = run['workflow_run_id']
    storage.update_run(identifier, lambda r: r.update(status='needs_input', input_decision_id='question-one', input_question='Choose a color'), 'test')
    storage.update_run(identifier, lambda r: r.update(status='completed', interaction_history=[{
        'interaction_id': 'question-one', 'action': 'resume', 'answer': 'Blue', 'answered_by': 'caller',
        'answered_at': time.time() + 1, 'workflow_run_id': identifier}]), 'test')
    events = cycle(logs, limit=1)
    assert [(e['kind'], e['state']) for e in events] == [('question', 'pending'), ('question', 'answered')]
    assert events[0]['question'] == 'Choose a color'
    assert events[1]['answer'] == 'Blue'
    assert events[1]['workflow_run_id'] == identifier


def test_cursor_validation_and_source_gaps(logs):
    with pytest.raises(ValueError, match='cursor'):
        feed.page(logs, '../outside')
    (logs / 'broken.meta.json').write_text('{}')
    result = feed.page(logs)
    assert result['history_incomplete']
    assert result['gaps'] == [{'source': 'broken', 'reason': 'source_unavailable'}]


def test_scheduler_loss_pending_is_stable_and_answer_reconstructs_between_polls(logs, monkeypatch):
    monkeypatch.setattr(w.backends, 'is_installed', lambda _: True)
    storage = w.WorkflowStore(logs.parent)
    run = storage.create_run(w.validate_definition(definition()), 'Goal', logs.parent)
    identifier = run['workflow_run_id']
    owner = {'pid': 123, 'start_time': 'generation', 'markers': []}
    storage.update_run(identifier, lambda r: r.update(status='running', supervisor_identity=owner, interaction_owner='monitor'), 'test')
    monkeypatch.setattr('harflow_mcp.identity.identity_check', lambda _: 'dead')
    first = cycle(logs)
    assert len(first) == 1
    assert first[0]['kind'] == 'recovery' and first[0]['monitor_available']
    assert first[0]['observed_at_read']
    assert cycle(logs)[0]['event_id'] == first[0]['event_id']
    storage.update_run(identifier, lambda r: r.update(status='completed', interaction_history=[{
        'interaction_id': first[0]['interaction_id'], 'action': 'recover', 'answer': 'Continue',
        'answered_by': 'monitor', 'answered_at': 101.0, 'workflow_run_id': identifier}]), 'test')
    later = cycle(logs)
    assert [e['state'] for e in later] == ['pending', 'answered']
    assert later[0]['event_id'] == first[0]['event_id']
    assert later[1]['answered_by'] == 'monitor'


def test_child_points_are_retained_independently_and_foreign_root_is_explicit_gap(logs, monkeypatch):
    from harflow_mcp import workflow_notification_points as points
    monkeypatch.setattr(w.backends, 'is_installed', lambda _: True)
    storage = w.WorkflowStore(logs.parent)
    run = storage.create_run(w.validate_definition(definition()), 'Goal', logs.parent)
    child = dict(run, parent_link={'workflow_run_id': 'missing-root', 'root_workflow_run_id': 'missing-root', 'execution_id': 'foreign'},
                 status='needs_input', input_decision_id='child-question', input_question='Child question')
    points.record_point(child, 'test')
    assert child['notification_points'][-1]['snapshot']['attention']['question'] == 'Child question'
    storage.update_run(run['workflow_run_id'], lambda r: r.update(child), 'test')
    result = feed.page(logs)
    assert result['events'] == []
    assert result['history_incomplete']


def test_read_only_feed_does_not_create_derived_catalog_on_corrupt_source(logs):
    task(logs)
    # Remove pre-existing derivative catalogs to make any accidental read-side write visible.
    for path in logs.parent.rglob('*catalog*'):
        if path.is_file():
            path.unlink()
    (logs / 'task-1.meta.json').write_text('{}')
    before = {str(path.relative_to(logs.parent)) for path in logs.parent.rglob('*')}
    assert feed.page(logs)['history_incomplete']
    after = {str(path.relative_to(logs.parent)) for path in logs.parent.rglob('*')}
    assert after == before


def test_symlinked_workflow_directory_is_not_read(logs, tmp_path):
    outside = tmp_path / 'outside'
    outside.mkdir()
    (outside / 'foreign.json').write_text('{}')
    (logs.parent / 'workflow-runs').symlink_to(outside, target_is_directory=True)
    result = feed.page(logs)
    assert result['events'] == []
    assert result['history_incomplete'] is True
    assert {'source': 'foreign', 'reason': 'source_unavailable'} in result['gaps']
    assert not list(outside.glob('.*'))


@pytest.mark.parametrize('status,action', [('failed', 'recover'), ('needs_attention', 'resume')])
def test_recovery_pending_precedes_opaque_answer_across_pages(logs, monkeypatch, status, action):
    monkeypatch.setattr(w.backends, 'is_installed', lambda _: True)
    storage = w.WorkflowStore(logs.parent)
    run = storage.create_run(w.validate_definition(definition()), 'Goal', logs.parent)
    identifier = run['workflow_run_id']
    storage.update_run(identifier, lambda r: r.update(status=status, attention_reason='Review evidence'), 'test')
    storage.update_run(identifier, lambda r: r.update(status='completed', interaction_history=[{
        'interaction_id': 'attention:opaque-checkpoint', 'action': action, 'answer': 'Continue',
        'answered_by': 'monitor', 'answered_at': time.time() + 1, 'workflow_run_id': identifier}]), 'test')
    events = cycle(logs, limit=1)
    assert [event['state'] for event in events] == ['pending', 'answered']
    assert len({event['event_id'] for event in events}) == 2


def test_byte_pruned_history_reports_gap_below_count_limit(logs, monkeypatch):
    monkeypatch.setattr(w.backends, 'is_installed', lambda _: True)
    storage = w.WorkflowStore(logs.parent)
    run = storage.create_run(w.validate_definition(definition()), 'Goal', logs.parent)
    storage.update_run(run['workflow_run_id'], lambda r: r.update(interaction_history_truncated=True), 'test')
    page = feed.page(logs)
    assert page['history_incomplete'] is True
    assert any(gap['reason'] == 'answer_history_at_retention_limit' for gap in page['gaps'])
