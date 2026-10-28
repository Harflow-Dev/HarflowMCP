"""Input arbitration reads only the selected root's verified invocation tree."""
import copy
import json
import uuid

import pytest
from harflow_mcp import state_ownership
from harflow_mcp import workflow_run_documents

from harflow_mcp import workflow_invocation as inv
from harflow_mcp import workflows
from test_workflow_run_node_recovery import finished_child, seed_parent, store, write_run


@pytest.fixture(autouse=True)
def owned_workflow_scope(store, monkeypatch):
    monkeypatch.setattr(
        "test_workflow_input_tree.write_run",
        lambda storage, run: __import__("harflow_mcp.workflows", fromlist=["_write"])._write(
            storage.runs / f"{run['workflow_run_id']}.json", workflow_run_documents.encode(run),
            layout=storage.state_layout,
        ),
    )
    with state_ownership.state_session(store.state_layout):
        yield


def question(store, parent, activation, *, created=0, decision='question'):
    child = finished_child(store, parent, activation, status='needs_input')
    child.update(created_at=created, input_decision_id=decision, input_question=decision)
    write_run(store, child)
    store._index_run(child)
    return child


def test_input_arbitration_never_reads_unrelated_retained_history(store, tmp_path, monkeypatch):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    first = question(store, root, root['activations'][0], created=1, decision='first')
    second_activation = copy.deepcopy(root['activations'][0])
    second_activation['id'] = uuid.uuid4().hex
    second_activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    store.update_run(root['workflow_run_id'], lambda r: r['activations'].append(second_activation), 'fixture')
    second = question(store, root, second_activation, created=2, decision='second')
    unrelated = store.runs / 'unrelated.json'
    unrelated.write_text('invalid retained history' * 250_000)
    monkeypatch.setattr(store, 'list_runs', lambda: pytest.fail('retained history scan'))
    original = store.get_run
    reads = []
    def read(identifier, **kwargs):
        reads.append(identifier)
        assert identifier != 'unrelated'
        return original(identifier, **kwargs)
    monkeypatch.setattr(store, 'get_run', read)
    inv.publish_input(store, second)
    published = original(root['workflow_run_id'])
    assert published['input_decision_id'] == 'first'
    assert published['input_source']['path'] == [root['workflow_run_id'], first['workflow_run_id']]
    assert set(reads) == {root['workflow_run_id'], first['workflow_run_id'], second['workflow_run_id']}
    # An already displayed question remains stable while all siblings are verified.
    reads.clear()
    inv.publish_input(store, second)
    assert set(reads) == {root['workflow_run_id'], first['workflow_run_id'], second['workflow_run_id']}
    store.control(root['workflow_run_id'], 'resume', 'first answer', decision_id='first')
    inv.publish_input(store, original(second['workflow_run_id']))
    assert original(root['workflow_run_id'])['input_decision_id'] == 'second'
    assert original(first['workflow_run_id'])['instructions'] == 'first answer'
    assert original(second['workflow_run_id'])['status'] == 'needs_input'
    assert unrelated.stat().st_size > 4 * 1024 * 1024


@pytest.mark.parametrize('replacement_count', [1, 2])
def test_input_arbitration_handles_bounded_atomic_refreshes(store, tmp_path, monkeypatch, replacement_count):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    active = question(store, root, root['activations'][0], decision='atomic-question')
    path = store.runs / f"{root['workflow_run_id']}.json"
    second = None
    if replacement_count == 2:
        second_activation = copy.deepcopy(root['activations'][0])
        second_activation['id'] = uuid.uuid4().hex
        second_activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
        store.update_run(root['workflow_run_id'], lambda value: value['activations'].append(second_activation), 'fixture')
        root = store.get_run(root['workflow_run_id'])
        second = question(store, root, second_activation, decision='second-question')
    original_fdopen = workflows.os.fdopen
    reads = 0
    replacements = 0

    class Stream:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): self.stream.__enter__(); return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def read(self, *args, **kwargs):
            nonlocal reads, replacements
            data = self.stream.read(*args, **kwargs)
            reads += 1
            if replacement_count == 1 and reads == 1:
                replacements += 1
                fresh = workflow_run_documents.decode(json.loads(data), root['workflow_run_id'])
                fresh['name'] = 'root after atomic refresh'
                temporary = path.with_suffix('.input-refresh.tmp')
                temporary.write_text(json.dumps(workflow_run_documents.encode(fresh)))
                temporary.replace(path)
            elif replacement_count == 2 and reads == 1:
                replacements += 1
                identifier = root['workflow_run_id']
                fresh = workflow_run_documents.decode(json.loads(data), identifier)
                fresh['prompt'] = f'atomic refresh {replacements}'
                target = store.runs / f'{identifier}.json'
                temporary = target.with_suffix(f'.input-refresh-{replacements}.tmp')
                temporary.write_text(json.dumps(workflow_run_documents.encode(fresh)))
                temporary.replace(target)
            elif replacement_count == 2 and reads in (4, 5):
                replacements += 1
                identifier = second['workflow_run_id']
                fresh = workflow_run_documents.decode(json.loads(data), identifier)
                fresh['prompt'] = f'atomic refresh {replacements}'
                target = store.runs / f'{identifier}.json'
                temporary = target.with_suffix('.input-refresh-second.tmp')
                temporary.write_text(json.dumps(workflow_run_documents.encode(fresh)))
                temporary.replace(target)
            return data

    monkeypatch.setattr(workflows.os, 'fdopen', lambda fd, *args, **kwargs: Stream(original_fdopen(fd, *args, **kwargs)))
    inv.publish_input(store, active)
    published = store.get_run(root['workflow_run_id'])
    assert replacements == (1 if replacement_count == 1 else 3)
    if replacement_count == 1:
        assert published['status'] == 'needs_input'
        assert published['input_decision_id'] == 'atomic-question'
    else:
        assert published['status'] == 'needs_input'
        assert published['input_decision_id'] == 'atomic-question'
    journal = (store.runs / f"{root['workflow_run_id']}.jsonl").read_text()
    assert 'child_input_arbitration_unavailable' not in journal


@pytest.mark.parametrize('field', ['workflow_run_id', 'execution_id', 'root_workflow_run_id'])
def test_input_arbitration_rejects_unverified_child_links(store, tmp_path, monkeypatch, field):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = question(store, root, root['activations'][0])
    child['parent_link'][field] = 'wrong'
    write_run(store, child)
    monkeypatch.setattr(store, 'list_runs', lambda: pytest.fail('retained history scan'))
    inv.publish_input(store, child)
    assert store.get_run(root['workflow_run_id'])['status'] == 'running'


def test_nested_input_path_and_duplicate_edges_are_verified_once(store, tmp_path, monkeypatch):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    parent = finished_child(store, root, root['activations'][0], status='running')
    parent['dependency_tree'] = root['dependency_tree']
    activation = copy.deepcopy(root['activations'][0])
    activation['id'] = uuid.uuid4().hex
    activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    parent['activations'] = [activation, copy.deepcopy(activation)]
    write_run(store, parent)
    child = question(store, parent, activation)
    child['parent_link']['root_workflow_run_id'] = root['workflow_run_id']
    write_run(store, child)
    store._index_run(child)
    original = store.get_run
    reads = []
    def read(identifier, **kwargs):
        reads.append(identifier)
        return original(identifier, **kwargs)
    monkeypatch.setattr(store, 'get_run', read)
    monkeypatch.setattr(store, 'list_runs', lambda: pytest.fail('retained history scan'))
    inv.publish_input(store, child)
    published = original(root['workflow_run_id'])
    assert published['input_source']['path'] == [root['workflow_run_id'], parent['workflow_run_id'], child['workflow_run_id']]
    assert reads.count(child['workflow_run_id']) == 1


def test_completed_loop_invocations_do_not_read_retained_children(store, tmp_path, monkeypatch):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    active = question(store, root, root['activations'][0])
    historical = []
    for index in range(150):
        identifier = uuid.uuid4().hex
        # Corrupt historical records prove even a cold cache never loads them.
        (store.runs / f'{identifier}.json').write_text('invalid' if index else 'x' * (5 * 1024 * 1024))
        historical_activation = copy.deepcopy(root['activations'][0])
        historical_activation.update(
            id=uuid.uuid4().hex,
            status='completed',
            tasks=[],
            invocation={**historical_activation['invocation'], 'child_workflow_run_id': identifier, 'stage': 'settled'},
            node_result={'status': 'succeeded', 'result': {'child_outcome': {'kind': 'completed'}}, 'evidence': []},
        )
        historical.append(historical_activation)
    store.update_run(root['workflow_run_id'], lambda r: r['activations'].extend(historical), 'fixture')
    blocked = {a['invocation']['child_workflow_run_id'] for a in historical}
    original = store.get_run
    def read(identifier, **kwargs):
        assert identifier not in blocked
        return original(identifier, **kwargs)
    monkeypatch.setattr(store, 'get_run', read)
    inv.publish_input(store, active)
    assert original(root['workflow_run_id'])['input_decision_id'] == active['input_decision_id']


@pytest.mark.parametrize('failure', ['missing', 'corrupt'])
def test_unavailable_active_branch_preserves_verified_question(store, tmp_path, failure):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    active = question(store, root, root['activations'][0])
    identifier = uuid.uuid4().hex
    broken = copy.deepcopy(root['activations'][0])
    broken['id'] = uuid.uuid4().hex
    broken['invocation']['child_workflow_run_id'] = identifier
    store.update_run(root['workflow_run_id'], lambda r: r['activations'].append(broken), 'fixture')
    if failure == 'corrupt':
        (store.runs / f'{identifier}.json').write_text('not JSON')
    inv.publish_input(store, active)
    actual = store.get_run(root['workflow_run_id'])
    assert actual['status'] == 'needs_input' and actual['input_decision_id'] == active['input_decision_id']
    from harflow_mcp.workflow_inputs import project, verified_source
    projected = project(store, actual)
    assert projected['pending_input_count'] == 1
    diagnostic = projected['pending_attention'][0]
    assert diagnostic['parent_workflow_run_id'] == root['workflow_run_id']
    assert diagnostic['child_workflow_run_id'] == identifier
    assert diagnostic['execution_id'] == broken['id']
    assert diagnostic['reason']
    assert not verified_source(store, actual, identifier)


def test_source_claim_cannot_publish_an_unrelated_roots_question(store, tmp_path):
    own = seed_parent(store, tmp_path, 'parent', stage='waiting')
    source = question(store, own, own['activations'][0], decision='own-question')
    unrelated = seed_parent(store, tmp_path, 'parent', stage='waiting')
    question(store, unrelated, unrelated['activations'][0], decision='unrelated-question')
    source['parent_link']['root_workflow_run_id'] = unrelated['workflow_run_id']
    inv.publish_input(store, source)
    assert store.get_run(unrelated['workflow_run_id'])['status'] == 'running'


@pytest.mark.parametrize('size', [5 * 1024 * 1024, 3 * 1024 * 1024])
def test_input_arbitration_budget_failure_is_recoverable_and_never_selects_partial_history(store, tmp_path, monkeypatch, size):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    activations = []
    for index in range(3):
        activation = copy.deepcopy(root['activations'][0])
        activation['id'] = uuid.uuid4().hex
        activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
        activations.append(activation)
        child = question(store, root, activation, created=index, decision=f'question-{index}')
        child['instructions'] = 'x' * (0 if size > 4 * 1024 * 1024 and index == 2 else size)
        write_run(store, child)
        store._index_run(child)
    store.update_run(root['workflow_run_id'], lambda r: r.update(activations=activations), 'fixture')
    original = store.get_run
    used = []
    def read(identifier, **kwargs):
        budget = kwargs.get('metadata_budget')
        try:
            if budget is not None:
                assert kwargs['metadata_byte_limit'] == 4 * 1024 * 1024
            return original(identifier, **kwargs)
        finally:
            if budget is not None:
                used.append(budget.metadata_bytes)
    monkeypatch.setattr(store, 'get_run', read)
    inv.publish_input(store, child)
    actual = store.get_run(root['workflow_run_id'])
    assert actual['status'] == 'needs_attention'
    assert 'bounded metadata arbitration' in actual['attention_reason']
    assert not actual.get('input_decision_id')
    # The same invocation reservation remains available for explicit retry.
    assert [a['id'] for a in actual['activations']] == [a['id'] for a in activations]
    assert used and max(used) <= 8 * 1024 * 1024


def test_all_sibling_questions_answer_in_reverse_order(store, tmp_path, monkeypatch):
    from harflow_mcp.workflow_inputs import project
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    first = question(store, root, root['activations'][0], created=1, decision='first')
    second_activation = copy.deepcopy(root['activations'][0])
    second_activation['id'] = uuid.uuid4().hex
    second_activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    store.update_run(root['workflow_run_id'], lambda r: r['activations'].append(second_activation), 'fixture')
    second = question(store, root, second_activation, created=2, decision='second')
    inv.publish_input(store, first)
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    assert {x['decision_id'] for x in project(store, root)['pending_inputs']} == {'first', 'second'}
    result = store.control(root['workflow_run_id'], 'resume', 'second answer', decision_id='second')
    assert result['input_decision_id'] == 'first'
    assert result['input_delivery_receipt']['decision_id'] == 'second'
    accepted = result['input_delivery_receipt']['accepted_at']
    replay = store.control(root['workflow_run_id'], 'resume', 'second answer', decision_id='second')
    assert replay['input_delivery_receipt']['accepted_at'] == accepted
    assert project(store, result)['pending_input_count'] == 1
    result = store.control(root['workflow_run_id'], 'resume', 'first answer', decision_id='first')
    assert project(store, result)['pending_input_count'] == 0
    assert store.get_run(second['workflow_run_id'])['instructions'] == 'second answer'


def test_child_noop_does_not_accept_or_clear_root_question(store, tmp_path, monkeypatch):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = question(store, root, root['activations'][0], decision='first')
    inv.publish_input(store, child)
    original = store.control
    def control(identifier, *args, **kwargs):
        if identifier == child['workflow_run_id']:
            return store.get_run(identifier)
        return original(identifier, *args, **kwargs)
    monkeypatch.setattr(store, 'control', control)
    with pytest.raises(workflows.WorkflowError, match='durably accept'):
        store.control(root['workflow_run_id'], 'resume', 'answer', decision_id='first')
    assert store.get_run(root['workflow_run_id'])['input_decision_id'] == 'first'


def test_concurrent_sibling_answers_keep_durable_receipts(store, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from harflow_mcp.workflow_inputs import project
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    children = [question(store, root, root['activations'][0], decision='one')]
    for decision in ('two', 'three'):
        activation = copy.deepcopy(root['activations'][0])
        activation['id'] = uuid.uuid4().hex
        activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
        store.update_run(root['workflow_run_id'], lambda r, a=activation: r['activations'].append(a), 'fixture')
        children.append(question(store, root, activation, decision=decision))
    inv.publish_input(store, children[0])
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(lambda child: store.control(root['workflow_run_id'], 'resume', 'answer '+child['input_decision_id'], decision_id=child['input_decision_id']), children))
    assert project(store, store.get_run(root['workflow_run_id']))['pending_input_count'] == 0
    final = store.get_run(root['workflow_run_id'])
    assert set(final['accepted_inputs']) == {'one', 'two', 'three'}
    assert 'input_decision_id' not in final
    for child in children:
        accepted = store.get_run(child['workflow_run_id'])
        assert child['input_decision_id'] in accepted['accepted_inputs']


def test_answering_descendant_preserves_roots_own_question(store, tmp_path, monkeypatch):
    from harflow_mcp.workflow_inputs import project
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = question(store, root, root['activations'][0], decision='child-question')
    store.update_run(root['workflow_run_id'], lambda r: r.update(status='needs_input', input_decision_id='root-question', input_question='Root question'), 'fixture')
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    assert project(store, root)['pending_input_count'] == 2
    result = store.control(root['workflow_run_id'], 'resume', 'child answer', decision_id='child-question')
    assert result['input_decision_id'] == 'root-question'
    assert result['input_question'] == 'Root question'
    assert project(store, result)['pending_input_count'] == 1


@pytest.mark.parametrize('field,value', [('accepted_inputs', {'q': {'accepted_at': 'bad'}}), ('input_deliveries', {'q': {'id': 9}}), ('input_delivery_receipt', {'accepted_at': 'bad'}), ('input_question_items', [1])])
def test_input_ledger_known_fields_are_qualified(store, tmp_path, field, value):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    root[field] = value
    with pytest.raises(workflow_run_documents.WorkflowRunDocumentError):
        workflow_run_documents.encode(root)


def test_old_delivery_replays_after_source_accepts_new_decision(store, tmp_path, monkeypatch):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = question(store, root, root['activations'][0], decision='old')
    inv.publish_input(store, child)
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    first = store.control(root['workflow_run_id'], 'resume', 'old answer', decision_id='old')
    accepted_at = first['input_delivery_receipt']['accepted_at']
    store.update_run(child['workflow_run_id'], lambda r: r.update(status='needs_input', input_decision_id='new', input_question='New question'), 'fixture')
    inv.publish_input(store, store.get_run(child['workflow_run_id']))
    store.control(root['workflow_run_id'], 'resume', 'new answer', decision_id='new')
    replay = store.control(root['workflow_run_id'], 'resume', 'old answer', decision_id='old')
    assert replay['input_delivery_receipt']['accepted_at'] == accepted_at
    assert store.get_run(child['workflow_run_id'])['instructions'] == 'new answer'


def nested_question(store, tmp_path, decision='nested'):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    parent = finished_child(store, root, root['activations'][0], status='running')
    activation = copy.deepcopy(root['activations'][0])
    activation['id'] = uuid.uuid4().hex
    activation['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    parent['activations'] = [activation]
    write_run(store, parent)
    child = question(store, parent, activation, decision=decision)
    child['parent_link']['root_workflow_run_id'] = root['workflow_run_id']
    write_run(store, child)
    return root, parent, child


@pytest.mark.parametrize('recovery_key,worker_metadata', [('orchestrator_usage_recovery', 'evidence'), ('worker_usage_recovery', 'evidence'), ('worker_usage_recovery', 'failure_result'), ('worker_usage_recovery', 'candidate')])
@pytest.mark.asyncio
async def test_depth_two_usage_question_survives_compact_and_wait(store, tmp_path, monkeypatch, recovery_key, worker_metadata):
    from types import SimpleNamespace
    from harflow_mcp import server
    from harflow_mcp.workflow_reads import on_demand_projection
    from harflow_mcp.workflow_responses import compact
    root, parent, child = nested_question(store, tmp_path)
    full = 'Complete usage limit question. ' * 500
    child.update(input_question=full, input_question_items=['pause_workflow', 'cancel_workflow'])
    child[recovery_key] = {'decision_id': 'nested', 'failure_diagnostic': {'category': 'usage_limit'}}
    if recovery_key == 'orchestrator_usage_recovery':
        child[recovery_key]['failed_candidate'] = {'backend': 'codex', 'model': 'test-model'}
    else:
        child[recovery_key].update(node_id='failed-worker', execution_id='failed-execution')
        attempts = [
            {'task_id': 'earlier-task', 'candidate': {'backend': 'claude', 'model': 'earlier-model'}, 'status': 'failed', 'result': {'backend': 'claude', 'model': 'earlier-model', 'failure_diagnostic': {'category': 'usage_limit'}}},
            {'task_id': 'quota-task', 'candidate': {'backend': 'codex', 'model': 'configured-model'}, 'status': 'failed', 'result': {'backend': 'codex', 'model': 'test-model', 'failure_diagnostic': {'category': 'usage_limit'}}},
            {'task_id': 'later-task', 'candidate': {'backend': 'vibe', 'model': 'later-model'}, 'status': 'failed', 'result': {'backend': 'vibe', 'model': 'later-model'}},
        ]
        child['activations'] = [{'id': 'failed-execution', 'node_id': 'failed-worker', 'role': 'node', 'status': 'failed', 'created_at': 0.0, 'token': {'id': 'failed-token'}, 'tasks': attempts, 'node_result': {'status': 'failed', 'result': {'failure_kind': 'usage_limit'}, 'evidence': [{'task_id': 'quota-task', 'status': 'failed', 'failure_diagnostic': {'category': 'usage_limit'}}]}}]
        if worker_metadata == 'failure_result':
            child['activations'][0]['node_result']['evidence'] = []
        elif worker_metadata == 'candidate':
            attempts[1]['result'].pop('backend')
            attempts[1]['result'].pop('model')
            attempts[1]['candidate']['model'] = 'test-model'
    write_run(store, child)
    for _ in range(3):
        projected = on_demand_projection(store, store.get_run(root['workflow_run_id']))
        assert 'usage_limit: codex / test-model' in projected['attention_reason']
        row = compact(projected)['pending_inputs'][0]
        assert row['question'] == full
        assert row['kind'] == 'usage_limit'
        assert (row['backend'], row['model']) == ('codex', 'test-model')
        if recovery_key == 'worker_usage_recovery':
            assert row['node_id'] == 'failed-worker'
        assert row['resume_hint']
        assert row['path'] == [root['workflow_run_id'], parent['workflow_run_id'], child['workflow_run_id']]
    async def status(*args, **kwargs):
        return on_demand_projection(store, store.get_run(root['workflow_run_id']))
    monkeypatch.setattr(server, '_workflow_call', status)
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(log_dir=store.runs.parent / 'tasks'))
    waited = await server.wait_for_workflow(root['workflow_run_id'], timeout_seconds=0)
    assert {k: v for k, v in waited['pending_inputs'][0].items() if k != 'waiting_seconds'} == {k: v for k, v in row.items() if k != 'waiting_seconds'}
    assert store.get_run(root['workflow_run_id'])['status'] == 'running'


def test_depth_two_answer_routes_when_root_stored_running(store, tmp_path, monkeypatch):
    root, parent, child = nested_question(store, tmp_path)
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    result = store.control(root['workflow_run_id'], 'resume', 'nested answer', decision_id='nested')
    assert result['input_delivery_receipt']['workflow_run_id'] == child['workflow_run_id']
    assert store.get_run(child['workflow_run_id'])['instructions'] == 'nested answer'


@pytest.mark.parametrize('failure', ['preparing', 'missing', 'corrupt', 'foreign'])
def test_unavailable_stale_decision_never_falls_back_to_root_status(store, tmp_path, monkeypatch, failure):
    from harflow_mcp.workflow_inputs import project, verified_source
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = question(store, root, root['activations'][0], decision='stale')
    inv.publish_input(store, child)
    if failure == 'preparing':
        store.update_run(root['workflow_run_id'], lambda r: r['activations'][0]['invocation'].update(stage='preparing'), 'fixture')
    elif failure == 'foreign':
        child['parent_link']['execution_id'] = 'foreign'
        write_run(store, child)
    elif failure == 'corrupt':
        (store.runs / f"{child['workflow_run_id']}.json").write_text('bad')
    else:
        (store.runs / f"{child['workflow_run_id']}.json").unlink()
    monkeypatch.setattr(workflows, '_launch', lambda *args: pytest.fail('unexpected launch'))
    assert not verified_source(store, root, child['workflow_run_id'])
    if failure == 'preparing':
        assert project(store, root)['pending_attention_count'] == 0
    with pytest.raises(workflows.WorkflowError, match="decision_id 'stale'.*verified source.*retry"):
        store.control(root['workflow_run_id'], 'resume', 'answer', decision_id='stale')
    assert store.get_run(root['workflow_run_id'])['status'] == 'needs_input'


@pytest.mark.parametrize('failure', ['preparing', 'foreign'])
def test_preparing_or_foreign_sibling_keeps_valid_question(store, tmp_path, failure):
    from harflow_mcp.workflow_inputs import project, verified_source
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    active = question(store, root, root['activations'][0])
    sibling = copy.deepcopy(root['activations'][0])
    sibling['id'] = uuid.uuid4().hex
    sibling['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    if failure == 'preparing':
        sibling['invocation']['stage'] = 'preparing'
    else:
        foreign = question(store, root, sibling, decision='foreign')
        foreign['parent_link']['execution_id'] = 'other-execution'
        write_run(store, foreign)
    store.update_run(root['workflow_run_id'], lambda r: r['activations'].append(sibling), 'fixture')
    inv.publish_input(store, active)
    for _ in range(3):
        projected = project(store, root)
        assert [row['decision_id'] for row in projected['pending_inputs']] == ['question']
        assert projected['pending_attention_count'] == (failure == 'foreign')
        assert not verified_source(store, root, sibling['invocation']['child_workflow_run_id'])


def test_duplicate_verified_decisions_remain_hard_failure(store, tmp_path):
    from harflow_mcp.workflow_inputs import project
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    question(store, root, root['activations'][0])
    sibling = copy.deepcopy(root['activations'][0])
    sibling['id'] = uuid.uuid4().hex
    sibling['invocation']['child_workflow_run_id'] = uuid.uuid4().hex
    question(store, root, sibling)
    store.update_run(root['workflow_run_id'], lambda r: r['activations'].append(sibling), 'fixture')
    with pytest.raises(workflows.WorkflowError, match='ambiguous'):
        project(store, root)


def test_input_projection_budget_failure_never_returns_partial_questions(store, tmp_path, monkeypatch):
    from harflow_mcp.bounded_io import ReadLimit
    from harflow_mcp.workflow_inputs import project
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    child = question(store, root, root['activations'][0])
    read = store.get_run
    def limited(identifier, **kwargs):
        if identifier == child['workflow_run_id']:
            raise ReadLimit('Metadata decode request limit reached')
        return read(identifier, **kwargs)
    monkeypatch.setattr(store, 'get_run', limited)
    with pytest.raises(ReadLimit, match='request limit'):
        project(store, root)


def caller_owned_nested_question(store, tmp_path):
    root, parent, child = nested_question(store, tmp_path)
    evidence = {'kind': 'external_parent', 'identity': {'pid': 1234, 'start_time': 'qualified-client', 'markers': []}}
    root = store.update_run(root['workflow_run_id'], lambda r: r.update(interaction_owner='caller', interaction_caller=evidence), 'fixture')
    return root, parent, child


@pytest.mark.parametrize('reason', ['pid_absent', 'start_time_differs'])
def test_monitor_answers_depth_two_after_qualified_caller_death(store, tmp_path, monkeypatch, reason):
    from harflow_mcp import identity, workflow_interactions
    root, parent, child = caller_owned_nested_question(store, tmp_path)
    assert root['status'] == 'running'
    monkeypatch.setattr(identity, 'check_detail', lambda evidence: ('dead', reason))
    launches = []
    monkeypatch.setattr(workflows, '_launch', lambda storage, identifier: launches.append(identifier))
    result = store.control(root['workflow_run_id'], 'resume', 'human answer', decision_id='nested', interaction_owner='monitor')
    persisted = store.get_run(root['workflow_run_id'], refresh_atomic=True)
    assert persisted['interaction_owner'] == 'monitor'
    assert persisted['interaction_handover'] == result['interaction_handover']
    assert persisted['interaction_handover']['reason'] == reason
    assert persisted['interaction_handover']['action'] == 'resume'
    assert persisted['interaction_handover']['previous_owner'] == 'caller'
    assert persisted['interaction_handover']['responder'] == 'monitor'
    assert persisted['interaction_handover']['timestamp'] > 0
    assert store.get_run(child['workflow_run_id'])['instructions'] == 'human answer'
    assert launches == [root['workflow_run_id']]
    for descendant in (parent, child):
        current = store.get_run(descendant['workflow_run_id'])
        assert workflow_interactions.projection(store, current)['interaction_state']['owner'] == 'monitor'
    # The original caller returning cannot replay or replace the accepted answer.
    for responder in ('caller', None):
        with pytest.raises(workflows.WorkflowError, match='original caller|retry its original answer and grants'):
            store.control(root['workflow_run_id'], 'resume', 'human answer', decision_id='nested', interaction_owner=responder)
    assert len(launches) == 1


@pytest.mark.parametrize('verdict,reason', [('alive', 'start_time_match'), ('undecidable', 'ps_failed'), ('dead', 'legacy_markers_not_seen')])
def test_monitor_cannot_answer_depth_two_while_caller_alive_or_unknown(store, tmp_path, monkeypatch, verdict, reason):
    from harflow_mcp import identity
    root, parent, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda evidence: (verdict, reason))
    monkeypatch.setattr(workflows, '_launch', lambda *args: pytest.fail('unexpected launch'))
    with pytest.raises(workflows.WorkflowError, match='original caller'):
        store.control(root['workflow_run_id'], 'resume', 'human answer', decision_id='nested', interaction_owner='monitor')
    current = store.get_run(root['workflow_run_id'])
    assert current['interaction_owner'] == 'caller'
    assert not current.get('interaction_handover')
    assert not current.get('input_deliveries')
    assert not store.get_run(child['workflow_run_id']).get('accepted_inputs')


def test_caller_and_monitor_race_depth_two_answer_accepts_once(store, tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from harflow_mcp import identity
    root, parent, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda evidence: ('dead', 'pid_absent'))
    launches = []
    monkeypatch.setattr(workflows, '_launch', lambda storage, identifier: launches.append(identifier))
    barrier = Barrier(2)
    def answer(responder):
        barrier.wait(timeout=5)
        try:
            return store.control(root['workflow_run_id'], 'resume', responder + ' answer', decision_id='nested', interaction_owner=responder)
        except workflows.WorkflowError:
            return None
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(answer, ['caller', 'monitor']))
    assert sum(result is not None for result in results) == 1
    accepted = store.get_run(child['workflow_run_id'])['accepted_inputs']
    assert list(accepted) == ['nested']
    assert len(launches) == 1
    persisted = store.get_run(root['workflow_run_id'])
    assert persisted['input_delivery_receipt']['delivery_id'] == accepted['nested']['delivery']['id']


@pytest.mark.parametrize('responder', ['caller', None])
def test_returning_caller_cannot_answer_new_decision_after_monitor_handover(store, tmp_path, monkeypatch, responder):
    from harflow_mcp import identity
    root, parent, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda evidence: ('dead', 'pid_absent'))
    launches = []
    monkeypatch.setattr(workflows, '_launch', lambda storage, identifier: launches.append(identifier))
    store.control(root['workflow_run_id'], 'resume', 'human answer', decision_id='nested', interaction_owner='monitor')
    store.update_run(child['workflow_run_id'], lambda r: r.update(status='needs_input', input_decision_id='new-question', input_question='Another question'), 'fixture')
    with pytest.raises(workflows.WorkflowError, match='original caller'):
        store.control(root['workflow_run_id'], 'resume', 'returning caller answer', decision_id='new-question', interaction_owner=responder)
    persisted = store.get_run(child['workflow_run_id'])
    assert 'new-question' not in persisted.get('accepted_inputs', {})
    assert persisted['status'] == 'needs_input'
    assert len(launches) == 1


@pytest.mark.parametrize('protected', [False, True])
def test_internal_child_answer_after_monitor_handover_uses_root_authority(store, tmp_path, monkeypatch, protected):
    from harflow_mcp import identity
    root, parent, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda evidence: ('dead', 'pid_absent'))
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    store.control(root['workflow_run_id'], 'resume', 'human handover answer', decision_id='nested', interaction_owner='monitor')
    def new_question(run):
        run.update(status='needs_input', input_decision_id='ordinary-next', input_question='Choose a source file', interaction_owner='caller')
        if protected:
            run['worker_usage_recovery'] = {'decision_id': 'ordinary-next', 'node_id': 'failed-worker', 'execution_id': 'failed-execution', 'failure_diagnostic': {'category': 'usage_limit'}}
    store.update_run(child['workflow_run_id'], new_question, 'fixture')
    source = {'workflow_run_id': child['workflow_run_id'], 'workflow_name': child['name'], 'path': [root['workflow_run_id'], parent['workflow_run_id'], child['workflow_run_id']]}
    delivery = {'id': uuid.uuid4().hex, 'payload': {'source_id': child['workflow_run_id'], 'action': 'resume', 'instructions': 'Use the existing source file', 'additional_attempts': 0, 'decision_id': 'ordinary-next', 'allow_optional_review_skip': False, 'interaction_owner': 'caller', 'source': source}}
    def answer():
        return store.control(child['workflow_run_id'], 'resume', delivery['payload']['instructions'], decision_id='ordinary-next', interaction_owner='caller', _internal=True, _ordinary_answer=True, _delivery=delivery)
    if protected:
        with pytest.raises(workflows.WorkflowError, match='requires caller authority'):
            answer()
        persisted = store.get_run(child['workflow_run_id'])
        assert 'ordinary-next' not in persisted.get('accepted_inputs', {})
        assert persisted.get('forwarded_delivery_receipt') != delivery
        assert persisted['status'] == 'needs_input'
    else:
        answer()
        persisted = store.get_run(child['workflow_run_id'], refresh_atomic=True)
        assert persisted['accepted_inputs']['ordinary-next']['delivery'] == delivery
        assert persisted['forwarded_delivery_receipt'] == delivery
        assert persisted['instructions'] == delivery['payload']['instructions']
        assert persisted['interaction_owner'] == 'caller'
        # The retained child owner is historical; its authority comes from root.
        assert store.get_run(root['workflow_run_id'])['interaction_owner'] == 'monitor'


@pytest.mark.parametrize('status', ['paused', 'needs_attention', 'needs_input'])
@pytest.mark.parametrize('local_source', [False, True])
def test_exact_retained_local_recovery_checkpoint_resumes_without_question_text(store, tmp_path, monkeypatch, status, local_source):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    def checkpoint(run):
        run.update(status=status, activations=[], pending=[], input_decision_id='local-recovery', worker_usage_recovery={'decision_id': 'local-recovery', 'node_id': 'work', 'execution_id': 'settled-work', 'failure_diagnostic': {'category': 'usage_limit'}})
        run.pop('input_question', None)
        if local_source:
            run['input_source'] = {'workflow_run_id': root['workflow_run_id'], 'workflow_name': root['name'], 'path': [root['workflow_run_id']]}
    store.update_run(root['workflow_run_id'], checkpoint, 'fixture')
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    result = store.control(root['workflow_run_id'], 'resume', 'Use the configured fallback', decision_id='local-recovery')
    assert result['status'] == 'running'
    assert result['instructions'] == 'Use the configured fallback'
    assert 'input_decision_id' not in result
    assert 'worker_usage_recovery' not in result


@pytest.mark.parametrize('checkpoint', ['running', 'stale', 'accepted', 'foreign', 'paused_without_recovery', 'paused_stale_recovery'])
def test_local_checkpoint_route_does_not_restore_generic_root_fallback(store, tmp_path, monkeypatch, checkpoint):
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    def prepare(run):
        run.update(status='needs_input', activations=[], pending=[], input_decision_id='local-recovery')
        run.pop('input_question', None)
        if checkpoint == 'running':
            run['status'] = 'running'
        elif checkpoint == 'accepted':
            run['accepted_inputs'] = {'local-recovery': {'accepted_at': 1.0}}
        elif checkpoint in {'paused_without_recovery', 'paused_stale_recovery'}:
            run['status'] = 'paused'
            if checkpoint == 'paused_stale_recovery':
                run['worker_usage_recovery'] = {'decision_id': 'old', 'node_id': 'work', 'execution_id': 'settled-work', 'failure_diagnostic': {'category': 'usage_limit'}}
        elif checkpoint == 'foreign':
            run['input_source'] = {'workflow_run_id': uuid.uuid4().hex, 'workflow_name': 'unverified', 'path': [root['workflow_run_id']]}
    store.update_run(root['workflow_run_id'], prepare, 'fixture')
    monkeypatch.setattr(workflows, '_launch', lambda *args: pytest.fail('unexpected launch'))
    decision = 'stale' if checkpoint == 'stale' else 'local-recovery'
    with pytest.raises(workflows.WorkflowError, match=f"Input decision_id '{decision}'.*not currently available from verified source.*retry"):
        store.control(root['workflow_run_id'], 'resume', 'answer', decision_id=decision)


@pytest.mark.parametrize('initial_status', ['paused', 'needs_input'])
@pytest.mark.parametrize('moved_checkpoint', ['accepted_and_paused', 'foreign_source'])
def test_root_local_checkpoint_revalidated_under_answer_lock(store, tmp_path, monkeypatch, initial_status, moved_checkpoint):
    from harflow_mcp import workflow_inputs
    root = seed_parent(store, tmp_path, 'parent', stage='waiting')
    def prepare(run):
        run.update(status=initial_status, activations=[], pending=[], input_decision_id='quota-X', instructions='last valid answer', worker_usage_recovery={'decision_id': 'quota-X', 'node_id': 'work', 'execution_id': 'settled-work', 'failure_diagnostic': {'category': 'usage_limit'}})
        if initial_status == 'needs_input':
            run['input_question'] = 'Quota question selected by project'
    store.update_run(root['workflow_run_id'], prepare, 'fixture')
    original_project = workflow_inputs.project
    foreign = uuid.uuid4().hex
    def project_then_move(storage, selected, **kwargs):
        projected = original_project(storage, selected, **kwargs)
        def move(run):
            if moved_checkpoint == 'accepted_and_paused':
                run.update(status='paused', accepted_inputs={'quota-X': {'accepted_at': 1.0}})
                for key in ('input_decision_id', 'input_question', 'worker_usage_recovery'):
                    run.pop(key, None)
            else:
                run['input_source'] = {'workflow_run_id': foreign, 'workflow_name': 'foreign', 'path': [root['workflow_run_id'], foreign]}
        storage.update_run(root['workflow_run_id'], move, 'fixture_interleaved_answer')
        return projected
    monkeypatch.setattr(workflow_inputs, 'project', project_then_move)
    monkeypatch.setattr(workflows, '_launch', lambda *args: pytest.fail('stale route launched'))
    source_label = foreign if moved_checkpoint == 'foreign_source' else root['workflow_run_id']
    with pytest.raises(workflows.WorkflowError, match=f"Input decision_id 'quota-X'.*verified source {source_label}.*retry"):
        store.control(root['workflow_run_id'], 'resume', 'stale answer', additional_attempts=1, decision_id='quota-X')
    persisted = store.get_run(root['workflow_run_id'], refresh_atomic=True)
    assert persisted['instructions'] == 'last valid answer'
    assert not persisted.get('attempt_grants')
    assert not persisted.get('protocol_retry_authorizations')
    assert not persisted.get('blocker_retry_authorizations')
    assert 'input_delivery_receipt' not in persisted
    if moved_checkpoint == 'accepted_and_paused':
        assert persisted['status'] == 'paused'
        assert persisted['accepted_inputs'] == {'quota-X': {'accepted_at': 1.0}}
