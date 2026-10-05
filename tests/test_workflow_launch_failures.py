import pytest

from polybridge import workflows as w
from test_workflows import definition


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda _: True)
    return w.WorkflowStore(tmp_path)


@pytest.mark.parametrize("failure", ["open", "spawn"])
def test_definite_launch_failure_retains_owned_recoverable_run(store, tmp_path, monkeypatch, failure):
    run = store.create_run(w.validate_definition(definition()), "work", tmp_path)
    rid = run["workflow_run_id"]
    store.update_run(rid, lambda r: r.update(interaction_owner="monitor"), "owner")
    if failure == "open":
        (store.runs / f"{rid}.supervisor.log").mkdir()
    else:
        def fail(*args, **kwargs):
            raise OSError("spawn failed")
        monkeypatch.setattr(w.subprocess, "Popen", fail)
    with pytest.raises(w.WorkflowError, match=rid):
        w._launch(store, rid)
    updated = store.get_run(rid)
    assert updated["status"] == "needs_attention"
    assert updated["interaction_owner"] == "monitor"
    assert updated["supervisor_pid"] is None
    calls = []
    monkeypatch.setattr(w.subprocess, "Popen", lambda *args, **kwargs: calls.append(args))
    if failure == "open":
        (store.runs / f"{rid}.supervisor.log").rmdir()
    w._launch(store, rid)
    assert len(calls) == 1
    assert len(store.list_runs()) == 1


def test_ambiguous_launch_exception_does_not_claim_definite_failure(store, tmp_path, monkeypatch):
    run = store.create_run(w.validate_definition(definition()), "work", tmp_path)
    def interrupt(*args, **kwargs):
        raise KeyboardInterrupt()
    monkeypatch.setattr(w.subprocess, "Popen", interrupt)
    with pytest.raises(KeyboardInterrupt):
        w._launch(store, run["workflow_run_id"])
    assert store.get_run(run["workflow_run_id"])["status"] == "starting"


def test_guard_uses_index_and_blocks_legacy_active_without_completed_history_reads(store, tmp_path, monkeypatch):
    d = w.validate_definition(definition())
    for _ in range(120):
        run = store.create_run(d, "work", tmp_path)
        store.update_run(run["workflow_run_id"], lambda r: r.update(status="completed", instructions="x" * 50_000), "done")
    legacy = store.create_run(d, "legacy", tmp_path)
    rid = legacy["workflow_run_id"]
    def historical(r):
        r.pop("execution_contract", None)
    store.update_run(rid, historical, "legacy")
    for _ in range(4):
        if not store.list_run_page(active_only=True).get("bootstrap_pending"):
            break
    monkeypatch.setattr(store, "list_runs", lambda: pytest.fail("full history scan"))
    original = store.get_run
    reads = []
    def read(identifier, **kwargs):
        reads.append(identifier)
        return original(identifier, **kwargs)
    monkeypatch.setattr(store, "get_run", read)
    with pytest.raises(w.WorkflowError, match="Active historical"):
        w._require_no_active_historical_runs(store)
    assert not reads
    store.update_run(rid, lambda r: r.update(status="completed"), "done")
    reads.clear()
    w._require_no_active_historical_runs(store)
    assert not reads


@pytest.mark.parametrize("flag", ["bootstrap_pending", "history_incomplete"])
def test_guard_fails_closed_during_incomplete_index(store, monkeypatch, flag):
    monkeypatch.setattr(store, "list_run_page", lambda **kwargs: {flag: True})
    with pytest.raises(w.WorkflowError, match="incomplete"):
        w._require_no_active_historical_runs(store)


def test_historical_guard_is_indexed_for_more_than_one_page_of_modern_runs(store, tmp_path, monkeypatch):
    import json
    from polybridge import catalog
    d = w.validate_definition(definition())
    for _ in range(120):
        run = store.create_run(d, 'modern', tmp_path)
        # Large protected identifiers force nonprotected fields out of headers.
        store.update_run(run['workflow_run_id'], lambda r: r.update(name='🦋' * 200, parent_link={'workflow_run_id': 'x' * 128, 'root_workflow_run_id': 'y' * 128}, orchestrator_session_owner_run_id='z' * 128), 'large_header')
    for _ in range(3):
        store.list_run_page(active_only=True)
    query = "SELECT 1 FROM entries INDEXED BY active_historical WHERE active=1 AND COALESCE(json_extract(payload,'$.kind'),'workflow')!='builder' AND COALESCE(json_extract(payload,'$.execution_contract'),'')!='delegation' LIMIT 1"
    with store._ownership_catalog().connect() as db:
        assert any('active_historical' in row[3] for row in db.execute('EXPLAIN QUERY PLAN ' + query))
        assert all(json.loads(row[0])['execution_contract'] == 'delegation' for row in db.execute('SELECT payload FROM entries'))
    monkeypatch.setattr(store, 'list_runs', lambda: pytest.fail('retained history scan'))
    w._require_no_active_historical_runs(store)
    header = catalog.bound_header({'workflow_run_id': 'r', 'kind': 'workflow', 'execution_contract': 'delegation', **{f'large{i}': '🦋' * 200 for i in range(50)}})
    assert header['execution_contract'] == 'delegation'


async def test_public_resume_after_definite_start_failure_reuses_reserved_owner_and_id(store, tmp_path, monkeypatch):
    store.save('example', definition())
    store.list_run_page()
    calls = []
    def fail(*args, **kwargs):
        calls.append(args)
        raise OSError('definite failure')
    monkeypatch.setattr(w.subprocess, 'Popen', fail)
    with pytest.raises(w.WorkflowError, match='could not start'):
        await w.start_workflow('example', 'work', tmp_path, root=store.root, interaction_owner='monitor', _verified_caller=None)
    recorded = store.list_runs()
    assert len(recorded) == 1
    reserved = recorded[0]
    assert reserved['status'] == 'needs_attention' and reserved['interaction_owner'] == 'monitor'
    monkeypatch.setattr(w.subprocess, 'Popen', lambda *args, **kwargs: calls.append(args))
    resumed = store.control(reserved['workflow_run_id'], 'resume', interaction_owner='monitor')
    assert resumed['workflow_run_id'] == reserved['workflow_run_id'] and resumed['status'] == 'running'
    assert len(calls) == 2 and len(store.list_runs()) == 1


def test_definite_spawn_failure_does_not_overwrite_concurrently_advanced_outcome(store, tmp_path, monkeypatch):
    run = store.create_run(w.validate_definition(definition()), 'work', tmp_path)
    def advanced(*args, **kwargs):
        store.update_run(run['workflow_run_id'], lambda r: r.update(status='completed', supervisor_pid=12345), 'advanced')
        raise OSError('late failure')
    monkeypatch.setattr(w.subprocess, 'Popen', advanced)
    with pytest.raises(w.WorkflowError, match='could not start'):
        w._launch(store, run['workflow_run_id'])
    actual = store.get_run(run['workflow_run_id'])
    assert actual['status'] == 'completed' and actual['supervisor_pid'] == 12345
