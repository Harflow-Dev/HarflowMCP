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
        assert published['status'] == 'needs_attention'
        assert 'bounded metadata arbitration' in published['attention_reason']
    journal = (store.runs / f"{root['workflow_run_id']}.jsonl").read_text()
    assert ('child_input_arbitration_unavailable' in journal) is (replacement_count == 2)


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
def test_unavailable_active_branch_prevents_partial_question_arbitration(store, tmp_path, failure):
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
    assert actual['status'] == 'needs_attention' and not actual.get('input_decision_id')


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
