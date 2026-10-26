"""Observer continuity is durable metadata, separate from detached owner liveness."""
from dataclasses import replace

import pytest

from harflow_mcp import identity, store, supervision, task_documents
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from harflow_mcp.task_attachment import AttachedTask

class Registry:
    def __init__(self, owner):
        self._owner = owner

    def prune(self):
        pass


ORIGIN = {'pid': 101, 'start_time': 'origin-generation', 'markers': []}
OWNER = {'pid': 202, 'start_time': 'owner-generation', 'markers': []}
OTHER = {'pid': 303, 'start_time': 'observer-generation', 'markers': []}


@pytest.fixture
def saved(tmp_path, monkeypatch):
    monkeypatch.setattr(identity, 'own_identity', lambda: ORIGIN)
    monkeypatch.setattr(identity, 'identity_check', lambda _: 'alive')
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('alive', 'test'))
    with state_session(resolve_layout(root=tmp_path / 'state')):
        directory = tmp_path / 'state' / 'tasks'
        directory.mkdir()
        record = store.TaskRecord(task_id='observer-task', backend='claude', session_id='session',
            repo_path=str(tmp_path), started_at='2026-10-26T00:00:00+00:00',
            status='running', pid=404, start_time='agent-generation', markers=['agent'],
            owner=OWNER, dispatch_origin=ORIGIN)
        store.write(directory, record)
        yield directory, record


def test_fresh_durable_attachment_and_owner_read(saved):
    directory, record = saved
    registry = Registry(ORIGIN)
    # Recreate the handle, proving this does not depend on a fresh-dispatch memory flag.
    for task in (AttachedTask(directory, record.task_id, registry=registry),
                 AttachedTask(directory, record.task_id, registry=registry)):
        for payload in (task.snapshot(), task.brief()):
            assert payload['recovered'] is False
            assert payload['observation_mode'] == 'attached'
            assert payload['owner'] == OWNER
            assert payload['responding_server'] == {'pid': 101, 'start_time': 'origin-generation'}
            assert payload['output_capture'] == 'live'
        assert 'normal attachment' in task.snapshot()['note']
    payload = store.snapshot(directory, record, responding_server=OWNER)
    assert payload['observation_mode'] == 'owner'
    assert payload['recovered'] is False


@pytest.mark.parametrize('origin_state', ['alive', 'dead', 'undecidable'])
def test_other_observer_does_not_infer_owner_loss(saved, monkeypatch, origin_state):
    directory, record = saved
    monkeypatch.setattr(identity, 'identity_check', lambda value: origin_state if value.get('pid') == ORIGIN['pid'] else 'alive')
    registry = Registry(OTHER)
    task = AttachedTask(directory, record.task_id, registry=registry)
    for payload in (task.snapshot(), task.brief()):
        assert payload['recovered'] is True
        assert payload['observation_mode'] == 'recovered'
        assert payload['output_capture'] == 'live'
        assert payload['supervision']['owner_state'] == 'alive'
        assert payload['supervision']['dispatch_origin_state'] == origin_state
        assert payload['owner'] == OWNER
        assert 'does not imply owner loss' in payload['observation_note']


def test_catalog_projections_follow_current_reader_not_cached_writer(saved, monkeypatch):
    directory, record = saved
    # Metadata and catalog header were written by ORIGIN. No metadata rewrite between reads.
    for responder, mode in ((OTHER, 'recovered'), (ORIGIN, 'attached'), (OWNER, 'owner')):
        monkeypatch.setattr(identity, 'own_identity', lambda: responder)
        page = store.list_page(directory)
        item = next(item for item in page['items'] if item['task_id'] == record.task_id)
        assert item['observation_mode'] == mode
        assert item['recovered'] is (mode == 'recovered')
        assert item['responding_server']['pid'] == responder['pid']


@pytest.mark.parametrize('origin,responder', [(None, OTHER), (ORIGIN, {'pid': 101, 'start_time': None, 'markers': []})])
def test_missing_generation_retains_unknown_continuity(saved, monkeypatch, origin, responder):
    directory, record = saved
    record = replace(record, dispatch_origin=origin)
    store.write(directory, record)
    monkeypatch.setattr(identity, 'identity_check', lambda _: 'undecidable')
    payload = store.snapshot(directory, record, responding_server=responder)
    assert payload['recovered'] is True
    assert payload['observation_mode'] == 'unknown'
    assert payload['output_capture'] == 'unknown'
    assert 'cannot be determined' in payload['observation_note']


def test_reused_observer_pid_is_another_generation(saved):
    _, record = saved
    responder = dict(ORIGIN, start_time='new-generation')
    payload = supervision.observation_fields(record, responding_server=responder)
    assert payload['observation_mode'] == 'recovered'
    assert payload['recovered'] is True


def test_optional_origin_is_qualified(saved):
    _, record = saved
    from dataclasses import asdict
    document = asdict(record)
    document['dispatch_origin'] = {'pid': True, 'start_time': 'bad', 'markers': []}
    with pytest.raises(store.TaskDocumentError):
        task_documents.qualify_payload(document, record.task_id, store.TaskRecord)


def test_compact_status_keeps_observer_roles(saved):
    from harflow_mcp import task_reads
    directory, record = saved
    payload = store.snapshot(directory, record, responding_server=ORIGIN)
    brief = task_reads.brief_status(payload)
    assert brief['observation_mode'] == 'attached'
    assert brief['recovered'] is False
    assert brief['owner']['pid'] == OWNER['pid']
    assert brief['dispatch_origin']['pid'] == ORIGIN['pid']
    assert brief['responding_server']['pid'] == ORIGIN['pid']


def test_same_origin_observed_completion_does_not_claim_recovery(saved):
    directory, record = saved
    record = replace(record, status='completed', exit_code=0,
                     finished_at='2026-10-26T00:00:01+00:00', output_drained=True)
    store.write(directory, record)
    registry = Registry(ORIGIN)
    task = AttachedTask(directory, record.task_id, registry=registry)
    snapshot, brief = task.snapshot(), task.brief()
    for payload in (snapshot, brief):
        assert payload['recovered'] is False
        assert payload['observation_mode'] == 'attached'
        assert payload['output_capture'] == 'observed_terminal'
        assert 'Recovered from disk' not in payload.get('note', '')
    assert snapshot['note'] == snapshot['observation_note']


def test_current_origin_is_alive_without_origin_probe(saved, monkeypatch):
    directory, record = saved
    calls = []
    def check(value):
        calls.append(value)
        return 'alive'
    monkeypatch.setattr(identity, 'identity_check', check)
    payload = store.snapshot(directory, record, responding_server=ORIGIN)
    assert payload['supervision']['dispatch_origin_state'] == 'alive'
    assert all(value.get('pid') != ORIGIN['pid'] for value in calls)


@pytest.mark.parametrize('origin', [None, {'pid': 101, 'start_time': None, 'markers': []}])
def test_unqualified_origin_state_is_explicit_or_absent(saved, origin):
    directory, record = saved
    record = replace(record, dispatch_origin=origin)
    store.write(directory, record)
    payload = store.snapshot(directory, record, responding_server=OTHER)
    if origin is None:
        assert 'dispatch_origin_state' not in payload['supervision']
    else:
        assert payload['supervision']['dispatch_origin_state'] == 'undecidable'


def test_catalog_relationship_projection_does_not_probe_processes(saved, monkeypatch):
    _, record = saved
    def unexpected(_):
        raise AssertionError('catalog observation must not probe processes')
    monkeypatch.setattr(identity, 'identity_check', unexpected)
    assert supervision.observation_fields(record, responding_server=OTHER)['observation_mode'] == 'recovered'


def test_settled_brief_and_snapshot_add_no_dispatcher_process_probe(saved, monkeypatch):
    directory, record = saved
    record = replace(record, status='completed', exit_code=0,
                     finished_at='2026-10-26T00:00:01+00:00')
    store.write(directory, record)
    monkeypatch.setattr(identity, 'own_identity', lambda: OTHER)
    def forbidden(*args, **kwargs):
        pytest.fail('settled history must not spawn per-row process probes')
    monkeypatch.setattr(identity, 'identity_check', forbidden)
    monkeypatch.setattr(identity, 'check_detail', forbidden)
    for read in (store.brief, store.snapshot):
        payload = read(directory, record)
        assert payload['supervision']['dispatch_origin_state'] == 'undecidable'
        assert payload['output_capture'] == 'observed_terminal'
    # A matching responding process is already known alive without a subprocess lookup.
    payload = store.snapshot(directory, record, responding_server=ORIGIN)
    assert payload['supervision']['dispatch_origin_state'] == 'alive'
