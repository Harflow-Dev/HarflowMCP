"""Recordless native activity follows retention without inventing task records."""
import fcntl
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from harflow_mcp import retention
from harflow_mcp import workflow_run_documents
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout


@pytest.fixture
def tmp_path(tmp_path_factory: pytest.TempPathFactory):
    root = tmp_path_factory.mktemp("native-retention") / "state"
    with state_ownership.state_session(resolve_layout(root=root)):
        yield root


def fixture(tmp_path, **changes):
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=40)).timestamp()
    logs = tmp_path / 'tasks'
    logs.mkdir()
    runs = tmp_path / 'workflow-runs'
    runs.mkdir()
    task = dict(task_id='native-child', execution_kind='native_subagent', status='completed',
                dispatch_stage='child_settled', native_terminal=True, finished_at=old)
    task.update(changes)
    if task.get('finished_at') is None:
        task.pop('finished_at')
    run = dict(workflow_run_id='owner', kind='workflow', name='Retention', status='completed', created_at=old,
               updated_at=old, revision=0, sequence=0, transitions=0, prompt='', repo_path=str(tmp_path),
               freedom='write_in_repo', network=None, instructions='', permission_policy='legacy_ceiling',
               definition_hash='hash', definition={'name': 'Retention', 'workflow_id': 'wf', 'revision': 1,
               'orchestrator': {'backend': 'codex'}, 'nodes': [], 'connections': []},
               activations=[dict(id='activation', node_id='node', role='worker', status='completed',
                                created_at=old, tasks=[task])], decisions=[], tasks=[], sessions={}, pending=[], joins={},
               attempt_grants={}, retry_counts={}, retry_grants={}, suppressed_candidates=[], supervisor_pid=None)
    path = runs / 'owner.json'
    path.write_text(json.dumps(workflow_run_documents.encode(run)))
    event = logs / 'native-child.events.jsonl'
    event.write_text('evidence')
    os.utime(event, (old, old))
    return now, logs, path, event, run


@pytest.mark.parametrize('changes', [{}, dict(status='not_started', dispatch_stage='not_started', native_terminal=False)])
def test_settled_or_positive_prelaunch_refusal_ages_out(tmp_path, changes):
    now, logs, path, event, _ = fixture(tmp_path, **changes)
    unrelated = logs / 'native-child.stdout.jsonl'
    unrelated.write_text('not owned')
    stats = retention.sweep(logs, 30, now)
    assert not event.exists()
    assert path.exists() and unrelated.exists()
    assert stats['deleted_native_events'] == 1
    assert not (logs / 'native-child.meta.json').exists()


@pytest.mark.parametrize('changes', [dict(status='uncertain'), dict(status='running'),
    dict(native_terminal=False), dict(dispatch_stage='spawn_requested'), dict(finished_at=None)])
def test_unsettled_evidence_is_preserved(tmp_path, changes):
    now, logs, _, event, _ = fixture(tmp_path, **changes)
    retention.sweep(logs, 30, now)
    assert event.exists()


@pytest.mark.parametrize('young', ['finished_at', 'mtime'])
def test_both_terminal_time_and_log_bytes_must_age_out(tmp_path, young):
    now, logs, path, event, run = fixture(tmp_path)
    if young == 'finished_at':
        run['activations'][0]['tasks'][0]['finished_at'] = now.timestamp()
        path.write_text(json.dumps(workflow_run_documents.encode(run)))
    else:
        os.utime(event, None)
    retention.sweep(logs, 30, now)
    assert event.exists()


def test_active_linked_root_preserves_child(tmp_path):
    now, logs, path, event, run = fixture(tmp_path)
    run['parent_link'] = {'root_workflow_run_id': 'root'}
    path.write_text(json.dumps(workflow_run_documents.encode(run)))
    root = dict(run, workflow_run_id='root', status='running', created_at=0, updated_at=0, activations=[])
    (path.parent / 'root.json').write_text(json.dumps(workflow_run_documents.encode(root)))
    retention.sweep(logs, 30, now)
    assert event.exists()


def test_run_lock_preserves_log(tmp_path):
    now, logs, _, event, _ = fixture(tmp_path)
    key = hashlib.sha256(b'run:owner').hexdigest()
    with (tmp_path / f'.workflow-{key}.lock').open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        retention.sweep(logs, 30, now)
        assert event.exists()


def test_malformed_inventory_and_budget_preserve_evidence(tmp_path, monkeypatch):
    now, logs, path, event, run = fixture(tmp_path)
    run = workflow_run_documents.decode(json.loads(path.read_text()), "owner")
    run['activations'].append(dict(id='edge', node_id='node', role='worker', status='completed',
                                   created_at=run['created_at'], tasks=[],
                                   invocation={'child_workflow_run_id': 'bad'}))
    path.write_text(json.dumps(workflow_run_documents.encode(run)))
    (path.parent / 'bad.json').write_text('{}')
    with pytest.raises(workflow_run_documents.WorkflowRunDocumentError):
        retention.sweep(logs, 30, now)
    assert event.exists()
    run['activations'].pop()
    path.write_text(json.dumps(workflow_run_documents.encode(run)))
    (path.parent / 'bad.json').unlink()
    monkeypatch.setattr(retention, '_NATIVE_METADATA_BYTES', 1)
    retention.sweep(logs, 30, now)
    assert event.exists()


def test_unlink_failure_retains_discovery_for_retry(tmp_path, monkeypatch):
    now, logs, path, event, run = fixture(tmp_path)
    original = type(event).unlink
    with monkeypatch.context() as patch:
        def refuse(self, *args, **kwargs):
            if self == event:
                raise PermissionError('busy')
            return original(self, *args, **kwargs)
        patch.setattr(type(event), 'unlink', refuse)
        retention.sweep(logs, 30, now)
        assert event.exists() and path.exists()
    retention.sweep(logs, 30, now)
    assert not event.exists()


def test_resume_between_snapshot_and_lock_preserves_log(tmp_path, monkeypatch):
    now, logs, path, event, run = fixture(tmp_path)
    original = retention.fcntl.flock
    changed = False
    def resume(fd, operation):
        nonlocal changed
        if operation == fcntl.LOCK_EX | fcntl.LOCK_NB and not changed:
            changed = True
            run['status'] = 'running'
            path.write_text(json.dumps(workflow_run_documents.encode(run)))
        return original(fd, operation)
    monkeypatch.setattr(retention.fcntl, 'flock', resume)
    retention.sweep(logs, 30, now)
    assert event.exists()


def test_unrelated_history_larger_than_tree_budget_does_not_block_cleanup(tmp_path, monkeypatch):
    now, logs, path, event, run = fixture(tmp_path)
    for n in range(257):
        identifier = f'other-{n}'
        other = dict(run, workflow_run_id=identifier, status='completed', created_at=0, updated_at=0, activations=[])
        (path.parent / f'{identifier}.json').write_text(json.dumps(workflow_run_documents.encode(other)))
    retention.sweep(logs, 30, now)
    assert not event.exists()


def test_uncertain_descendant_preserves_root_evidence(tmp_path):
    now, logs, path, event, run = fixture(tmp_path)
    run['activations'].append(dict(id='edge', node_id='node', role='worker', status='completed',
                                   created_at=run['created_at'], tasks=[],
                                   invocation={'child_workflow_run_id': 'descendant'}))
    path.write_text(json.dumps(workflow_run_documents.encode(run)))
    child = dict(run, workflow_run_id='descendant', status='completed', created_at=0, updated_at=0,
                 parent_link=dict(workflow_run_id='owner', root_workflow_run_id='owner', execution_id='edge'),
                 activations=[dict(id='activation', node_id='node', role='worker', status='uncertain',
                                  created_at=0, tasks=[dict(task_id='uncertain-child', status='uncertain')])])
    (path.parent / 'descendant.json').write_text(json.dumps(workflow_run_documents.encode(child)))
    retention.sweep(logs, 30, now)
    assert event.exists()


@pytest.mark.parametrize('sibling', [dict(task_id='other', status='unrecognized'),
    dict(task_id='other', status='completed', execution_kind='native_subagent', native_terminal=False)])
def test_unknown_or_unproven_sibling_preserves_tree(tmp_path, sibling):
    now, logs, path, event, run = fixture(tmp_path)
    run['activations'][0]['tasks'].append(sibling)
    path.write_text(json.dumps(workflow_run_documents.encode(run)))
    retention.sweep(logs, 30, now)
    assert event.exists()
