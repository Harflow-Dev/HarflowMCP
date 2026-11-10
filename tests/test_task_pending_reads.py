"""Read-only pending-launch discovery over qualified isolated producer documents."""
import json
from pathlib import Path

import pytest

from harflow_mcp import ctl, lineage, store, task_pending_reads as pending, task_supervision, takeover
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from test_task_resources import write_receipt
from test_worktrees import record_for

_real_caller_refusal = takeover.caller_refusal
_real_detect_caller_detail = lineage.detect_caller_detail

A, B, C = 'a' * 32, 'b' * 32, 'c' * 32


@pytest.fixture
def layout(tmp_path, monkeypatch):
    selected = resolve_layout(root=tmp_path / 'state')
    monkeypatch.setattr(takeover, 'caller_refusal', lambda _, **kwargs: None)
    with state_session(selected):
        selected.tasks_directory.mkdir()
        yield selected


def receipt(layout, task_id=A, state='reserved', **changes):
    return write_receipt(layout.root.parent, task_id, state, **(dict(title='Fixture launch', created_at='2026-11-10T00:00:00+00:00') | changes))


@pytest.mark.parametrize('state', ['reserved', 'claimed', 'waiting_for_resources', 'granted', 'not_started'])
def test_prerecord_states_are_source_metadata_not_synthetic_tasks(layout, state):
    receipt(layout, state=state, resources={'names': ['fixture']}, not_started_reason='fixture reason')
    value = pending.pending_status(layout.tasks_directory, A)
    assert value['disposition'] == 'pending'
    assert value['launch']['state'] == state
    assert value['launch']['generation'] == 'c' * 32
    assert value['launch']['not_started_reason'] == 'fixture reason'
    assert value['cancel_support'] == ('unsupported' if state == 'not_started' else 'supported')
    assert not {'status', 'pid', 'owner', 'prompt', 'enforcement'} & value.keys()
    assert pending.pending_page(layout.tasks_directory)['items'][0] == {k: v for k, v in value.items() if k not in ('scope', 'observed_at')}


def test_resource_free_handshake_is_discoverable_but_cancel_unsupported(layout):
    receipt(layout)
    value = pending.pending_page(layout.tasks_directory)
    assert value['items'][0]['cancel_support'] == 'unsupported'
    assert value['coverage'] == 'complete' and not value['has_more']


@pytest.mark.parametrize('state', ['spawned', 'settled'])
def test_postrecord_without_record_is_not_absence_and_is_filtered_history(layout, state):
    receipt(layout, state=state)
    assert pending.pending_status(layout.tasks_directory, A)['disposition'] == 'unknown'
    page = pending.pending_page(layout.tasks_directory)
    assert page['items'] == [] and page['examined_count'] == 1


def test_exact_id_never_prefix_matches_and_recorded_requires_normal_status(layout):
    receipt(layout)
    assert pending.pending_status(layout.tasks_directory, 'a')['disposition'] == 'not_found'
    store.write(layout.tasks_directory, record_for(A, str(layout.root)))
    assert pending.pending_status(layout.tasks_directory, A)['disposition'] == 'recorded'
    assert pending.pending_page(layout.tasks_directory)['items'] == []


@pytest.mark.parametrize('kind', ['corrupt', 'symlink', 'unknown-state', 'wrong-binding', 'foreign'])
def test_unqualified_source_is_sanitized_deny_only(layout, kind):
    value = receipt(layout, title='PRIVATE TITLE', spawned_by=B)
    path = layout.tasks_directory / f'{A}.launch.json'
    if kind == 'corrupt':
        path.write_text('PRIVATE corrupt source')
    elif kind == 'symlink':
        path.unlink()
        path.symlink_to('/unavailable-private-fixture')
    else:
        if kind == 'unknown-state': value['state'] = 'future-state'
        if kind == 'wrong-binding': value['directory_identity'][1] += 1
        if kind == 'foreign': value['product'] = 'foreign'
        task_supervision._write(path, value)
    row = pending.pending_status(layout.tasks_directory, A)
    assert row['disposition'] == 'refused'
    assert set(row) == {'scope', 'observed_at', 'task_id', 'disposition', 'code', 'check'}
    assert 'PRIVATE' not in json.dumps(row) and str(layout.root) not in json.dumps(row)
    assert pending.pending_page(layout.tasks_directory)['coverage'] == 'unknown'


def test_invalid_launch_names_are_aggregate_uncertainty(layout):
    (layout.tasks_directory / '.invalid.launch.json').write_text('{}')
    page = pending.pending_page(layout.tasks_directory)
    assert page['items'] == [] and page['coverage'] == 'unknown'
    assert page['invalid_name_count'] == 1


def test_filtered_windows_advance_last_examined_and_finish(layout, monkeypatch):
    monkeypatch.setattr(pending, 'MAX_EXAMINED', 2)
    for task_id in (A, B, C): receipt(layout, task_id, state='settled')
    page = pending.pending_page(layout.tasks_directory)
    assert page['items'] == [] and page['has_more'] and page['examined_count'] == 2
    final = pending.pending_page(layout.tasks_directory, cursor=page['next_cursor'])
    assert final['items'] == [] and not final['has_more'] and final['examined_count'] == 1


def test_cursor_limit_scope_and_namespace_churn(layout):
    for task_id in (A, B): receipt(layout, task_id)
    page = pending.pending_page(layout.tasks_directory, limit=1)
    with pytest.raises(pending.PendingReadError, match='cursor_scope') as error:
        pending.pending_page(layout.tasks_directory, limit=2, cursor=page['next_cursor'])
    assert error.value.code == 'pending_cursor_invalid'
    receipt(layout, C)
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory, limit=1, cursor=page['next_cursor'])
    assert error.value.code == 'pending_cursor_expired'


def test_in_place_content_is_not_a_fake_global_revision(layout):
    for task_id in (A, B): receipt(layout, task_id)
    page = pending.pending_page(layout.tasks_directory, limit=1)
    receipt(layout, B, state='granted')
    final = pending.pending_page(layout.tasks_directory, limit=1, cursor=page['next_cursor'])
    assert final['namespace_revision'] == page['namespace_revision']
    assert final['items'][0]['launch']['state'] == 'granted'


@pytest.mark.parametrize('cap', ['MAX_ENTRIES', 'MAX_NAME_BYTES'])
def test_namespace_bounds_count_all_entries_and_do_not_issue_false_cursor(layout, monkeypatch, cap):
    receipt(layout)
    (layout.tasks_directory / 'unrelated').touch()
    monkeypatch.setattr(pending, cap, 1)
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'pending_observation_bound'


def test_authority_precedes_cursor_and_namespace(layout, monkeypatch):
    monkeypatch.setattr(takeover, 'caller_refusal', lambda _, **kwargs: ('caller_undecidable', 'private reason'))
    monkeypatch.setattr(pending, '_namespace', lambda *_: pytest.fail('must not inspect namespace'))
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory, cursor='invalid')
    assert error.value.code == 'caller_undecidable' and 'private reason' not in str(error.value)


def test_ctl_missing_root_does_not_create_and_env_refusal_precedes_root(tmp_path, monkeypatch, capsys):
    root = tmp_path / 'missing'
    monkeypatch.setenv('HARFLOW_STATE_ROOT', str(root))
    monkeypatch.setattr(takeover, 'env_agent_refusal', lambda: ('agent_caller', 'fixture'))
    assert ctl.main(['task-pending-page', '--json']) != 0
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'agent_caller'
    assert not root.exists()
    monkeypatch.setattr(takeover, 'env_agent_refusal', lambda: None)
    assert ctl.main(['task-pending-page', '--json']) != 0
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'pending_observation_unavailable'
    assert not root.exists()


def test_read_budget_no_progress_is_explicit_failure(layout, monkeypatch):
    receipt(layout)
    monkeypatch.setattr(pending, 'MAX_METADATA_BYTES', 1)
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'pending_observation_bound'


def test_response_bound_leaves_unreturned_candidate_for_next_page(layout, monkeypatch):
    for task_id in (A, B): receipt(layout, task_id, title='x' * 1000)
    monkeypatch.setattr(pending, 'MAX_RESPONSE_BYTES', 11000)
    page = pending.pending_page(layout.tasks_directory)
    assert len(page['items']) == 1 and page['bound'] == 'response_bytes'
    final = pending.pending_page(layout.tasks_directory, cursor=page['next_cursor'])
    assert final['items'][0]['task_id'] == B and not final['has_more']


def test_default_and_max_limits_make_progress_under_real_read_budget(layout):
    for index in range(101): receipt(layout, f'{index:032x}')
    default = pending.pending_page(layout.tasks_directory)
    assert len(default['items']) == 50 and default['has_more']
    maximum = pending.pending_page(layout.tasks_directory, limit=100)
    assert len(maximum['items']) == 100 and maximum['has_more']
    final = pending.pending_page(layout.tasks_directory, limit=100, cursor=maximum['next_cursor'])
    assert len(final['items']) == 1 and not final['has_more']


def test_metadata_bound_resumes_before_unread_candidate(layout, monkeypatch):
    for task_id in (A, B): receipt(layout, task_id)
    monkeypatch.setattr(pending, 'MAX_METADATA_BYTES', 65537)
    page = pending.pending_page(layout.tasks_directory)
    assert [row['task_id'] for row in page['items']] == [A]
    assert page['bound'] == 'read_budget' and page['coverage'] == 'unknown'
    final = pending.pending_page(layout.tasks_directory, cursor=page['next_cursor'])
    assert [row['task_id'] for row in final['items']] == [B]
    assert final['coverage'] == 'unknown'  # earlier uncertainty persists


def test_early_refusal_preserved_by_honest_traversal(layout):
    receipt(layout, A)
    (layout.tasks_directory / f'{A}.launch.json').write_text('{}')
    receipt(layout, B)
    first = pending.pending_page(layout.tasks_directory, limit=1)
    final = pending.pending_page(layout.tasks_directory, limit=1, cursor=first['next_cursor'])
    assert final['coverage'] == 'unknown' and not final['has_more']


def test_cross_root_and_malformed_cursors(layout, tmp_path):
    for task_id in (A, B): receipt(layout, task_id)
    cursor = pending.pending_page(layout.tasks_directory, limit=1)['next_cursor']
    other = resolve_layout(root=tmp_path / 'other')
    with state_session(other):
        other.tasks_directory.mkdir()
        with pytest.raises(pending.PendingReadError) as error:
            pending.pending_page(other.tasks_directory, limit=1, cursor=cursor)
        assert error.value.code == 'pending_cursor_invalid'
    for bad in ('invalid', 'e30=', pending._encode({'v': True}), 'x' * 5000):
        with pytest.raises(pending.PendingReadError) as error:
            pending.pending_page(layout.tasks_directory, cursor=bad)
        assert error.value.code == 'pending_cursor_invalid'


def test_namespace_churn_during_read_expires_instead_of_returning_partial_claim(layout, monkeypatch):
    receipt(layout, A)
    original = pending._lookup
    def changing(logs, task_id):
        result = original(logs, task_id)
        receipt(layout, B)
        return result
    monkeypatch.setattr(pending, '_lookup', changing)
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'pending_cursor_expired'


def test_spawn_race_is_recorded_only_from_qualified_record(layout, monkeypatch):
    receipt(layout)
    original = task_supervision.read_launch
    def spawned(logs, task_id):
        result = original(logs, task_id)
        store.write(logs, record_for(task_id, str(layout.root)))
        return result
    monkeypatch.setattr(task_supervision, 'read_launch', spawned)
    assert pending.pending_status(layout.tasks_directory, A)['disposition'] == 'recorded'


def test_replacement_during_qualified_source_read_is_sanitized(layout, monkeypatch):
    receipt(layout)
    original = task_supervision._launch_source
    def changed(logs, task_id):
        original(logs, task_id)
        raise task_supervision.LaunchDocumentError(logs, task_id, 'source_changed', 'private source path', unavailable=True)
    monkeypatch.setattr(task_supervision, '_launch_source', changed)
    value = pending.pending_status(layout.tasks_directory, A)
    assert value['disposition'] == 'refused' and value['check'] == 'source_changed'
    assert 'private source path' not in json.dumps(value)


def test_zero_deadline_is_unknown_bound_not_empty(layout, monkeypatch):
    receipt(layout)
    monkeypatch.setattr(pending, 'MAX_SECONDS', 0)
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'pending_observation_bound'


def test_no_liveness_repair_or_launch_scan_is_used(layout, monkeypatch):
    receipt(layout, state='claimed', owner={'pid': 987654321, 'start_time': None, 'markers': []})
    monkeypatch.setattr(task_supervision, 'scan_launches', lambda *_: pytest.fail('unbounded scan'))
    from harflow_mcp import identity
    monkeypatch.setattr(identity, 'identity_check', lambda *_: pytest.fail('liveness inference'))
    before = {p.name: p.read_bytes() for p in layout.tasks_directory.iterdir() if p.is_file()}
    result = pending.pending_status(layout.tasks_directory, A)
    after = {p.name: p.read_bytes() for p in layout.tasks_directory.iterdir() if p.is_file()}
    assert result['launch']['state'] == 'claimed' and before == after


def test_generated_fixture_bundle_matches_real_serializer_twice():
    import importlib.util
    path = Path(__file__).parents[1] / 'scripts/generate-pending-launch-fixtures.py'
    spec = importlib.util.spec_from_file_location('pending_fixture_generator', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    first = module.generate()
    assert first == module.generate()
    assert all(path.read_bytes() == raw for path, raw in first.items())
    status = json.loads((module.BUNDLE / 'ctl-task-pending-existing-status-pending.json').read_bytes())
    batch = json.loads((module.BUNDLE / 'ctl-task-pending-existing-batch-pending.json').read_bytes())
    assert status['error']['data']['launch_state'] == 'waiting_for_resources'
    assert batch['errors'][B]['data']['not_started_reason'] == 'Fixture pre-spawn refusal'


def test_real_source_replacement_is_detected_by_qualified_reader(layout, monkeypatch):
    value = receipt(layout)
    path = layout.tasks_directory / f'{A}.launch.json'
    original = task_supervision.os.fstat
    source_inode = path.stat().st_ino
    replaced = False
    def replacing(descriptor):
        nonlocal replaced
        info = original(descriptor)
        if not replaced and info.st_ino == source_inode:
            replaced = True
            task_supervision._write(path, value | {'generation': 'e' * 32})
        return info
    monkeypatch.setattr(task_supervision.os, 'fstat', replacing)
    result = pending.pending_status(layout.tasks_directory, A)
    assert result['disposition'] == 'refused' and result['check'] == 'source_changed'


def test_exact_response_bound_counts_serialized_control_character_expansion(layout, monkeypatch):
    receipt(layout, title='\x00' * 1024)
    monkeypatch.setattr(pending, 'MAX_RESPONSE_BYTES', 10000)
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_status(layout.tasks_directory, A)
    assert error.value.code == 'pending_observation_bound' and error.value.check == 'response_bytes'


def test_qualified_record_filters_even_an_obsolete_opaque_launch(layout):
    receipt(layout)
    (layout.tasks_directory / f'{A}.launch.json').write_text('{}')
    store.write(layout.tasks_directory, record_for(A, str(layout.root)))
    assert pending.pending_status(layout.tasks_directory, A)['disposition'] == 'recorded'
    assert pending.pending_page(layout.tasks_directory)['items'] == []


def test_maximum_valid_full_lineage_id_is_preserved(layout):
    full_id = 'q' * 64
    receipt(layout, spawned_by=full_id)
    result = pending.pending_status(layout.tasks_directory, A)
    assert result['launch']['lineage']['spawned_by'] == full_id
    assert not result['launch']['metadata_truncated']


def test_invalid_resource_names_never_become_trusted_display_metadata(layout):
    receipt(layout, resources={'names': ['PRIVATE RESOURCE LABEL']})
    result = pending.pending_status(layout.tasks_directory, A)
    assert result['disposition'] == 'refused' and result['check'] == 'resource_names'
    assert 'PRIVATE' not in json.dumps(result)


def test_active_worktree_kind_supports_existing_cancel_mechanism_without_exposing_path(layout):
    receipt(layout, worktree={'path': '/tmp/private-fixture-worktree'})
    result = pending.pending_status(layout.tasks_directory, A)
    assert result['cancel_support'] == 'supported'
    assert 'private-fixture-worktree' not in json.dumps(result)


def real_gate(monkeypatch):
    """Keep takeover+lineage detection real; control only OS process facts in isolated tests."""
    import os
    from harflow_mcp import identity
    for name in ('HARFLOW_TASK_ID', 'HARFLOW_DEPTH'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(takeover, 'caller_refusal', _real_caller_refusal)
    monkeypatch.setattr(lineage, 'detect_caller_detail', _real_detect_caller_detail)
    monkeypatch.setattr(lineage, '_default_process_table', lambda: {os.getpid(): 1})
    monkeypatch.setattr(identity, 'identity_check', lambda _: 'dead')


def test_real_caller_gate_bounds_all_entries_before_pending_discovery(layout, monkeypatch):
    real_gate(monkeypatch)
    receipt(layout)
    (layout.tasks_directory / 'unrelated-authority-entry').touch()
    monkeypatch.setattr(pending, 'MAX_ENTRIES', 1)
    monkeypatch.setattr(store, 'scan', lambda *_: pytest.fail('unbounded caller history scan'))
    monkeypatch.setattr(pending, '_namespace', lambda *_: pytest.fail('must refuse before pending discovery'))
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'caller_undecidable'


@pytest.mark.parametrize('bound', ['MAX_NAME_BYTES', 'MAX_METADATA_BYTES'])
def test_real_caller_gate_never_authorizes_partial_candidate_proof(layout, monkeypatch, bound):
    real_gate(monkeypatch)
    store.write(layout.tasks_directory, record_for(A, str(layout.root)))
    monkeypatch.setattr(pending, bound, 1)
    monkeypatch.setattr(store, 'scan', lambda *_: pytest.fail('unbounded caller history scan'))
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_status(layout.tasks_directory, B)
    assert error.value.code == 'caller_undecidable'


@pytest.mark.parametrize('verdict, expected', [('alive', 'agent_caller'), ('undecidable', 'caller_undecidable')])
def test_real_caller_gate_preserves_related_agent_and_uncertainty(layout, monkeypatch, verdict, expected):
    import os
    from harflow_mcp import identity
    real_gate(monkeypatch)
    store.write(layout.tasks_directory, record_for(A, str(layout.root), status='running', exit_code=None,
                                                 pid=987654321, pgid=os.getsid(0)))
    monkeypatch.setattr(identity, 'identity_check', lambda _: verdict)
    monkeypatch.setattr(store, 'scan', lambda *_: pytest.fail('unbounded caller history scan'))
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == expected


def test_real_caller_gate_refuses_unreadable_ordinary_history(layout, monkeypatch):
    real_gate(monkeypatch)
    (layout.tasks_directory / f'{A}.meta.json').write_text('{}')
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'caller_undecidable'


def test_real_caller_gate_human_success_never_uses_unbounded_scan(layout, monkeypatch):
    real_gate(monkeypatch)
    receipt(layout)
    store.write(layout.tasks_directory, record_for(B, str(layout.root), status='completed', exit_code=0))
    monkeypatch.setattr(store, 'scan', lambda *_: pytest.fail('unbounded caller history scan'))
    result = pending.pending_page(layout.tasks_directory)
    assert result['items'][0]['task_id'] == A


def test_real_gate_env_refusal_is_lazy_before_all_namespace_reads(layout, monkeypatch):
    real_gate(monkeypatch)
    monkeypatch.setenv('HARFLOW_TASK_ID', A)
    monkeypatch.setattr(pending, '_source_namespace', lambda *_: pytest.fail('env refusal must precede discovery'))
    with pytest.raises(pending.PendingReadError) as error:
        pending.pending_page(layout.tasks_directory)
    assert error.value.code == 'agent_caller'


def test_caller_refusal_optional_candidate_seam_keeps_default_call_compatible(layout, monkeypatch):
    calls = []
    def legacy_detector(log_dir):
        calls.append(log_dir)
        return lineage.Detection(None)
    monkeypatch.setattr(lineage, 'detect_caller_detail', legacy_detector)
    assert _real_caller_refusal(layout.tasks_directory, environ={}) is None
    assert calls == [layout.tasks_directory]
    supplied = []
    def candidate_detector(log_dir, *, candidates):
        supplied.append(candidates)
        return lineage.Detection(None, 'fixture undecidable')
    monkeypatch.setattr(lineage, 'detect_caller_detail', candidate_detector)
    assert _real_caller_refusal(layout.tasks_directory, environ={}, candidates=[])[0] == 'caller_undecidable'
    assert supplied == [[]]


def test_real_ctl_env_gate_and_missing_root_never_initialize_state(tmp_path, monkeypatch, capsys):
    real_gate(monkeypatch)
    root = tmp_path / 'absent-real-gate'
    monkeypatch.setenv('HARFLOW_STATE_ROOT', str(root))
    monkeypatch.setenv('HARFLOW_TASK_ID', A)
    assert ctl.main(['task-pending-status', '--task-id', B, '--json']) == 1
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'agent_caller'
    assert not root.exists()
    monkeypatch.delenv('HARFLOW_TASK_ID')
    assert ctl.main(['task-pending-page', '--json']) == 1
    assert json.loads(capsys.readouterr().out)['error']['code'] == 'pending_observation_unavailable'
    assert not root.exists()
