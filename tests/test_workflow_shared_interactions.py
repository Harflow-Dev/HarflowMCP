"""Shared responder CAS receipts survive competing answers and scheduler restarts."""
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from harflow_mcp import identity, workflow_attention, workflow_interactions as interactions, workflows
from test_workflow_attention_checkpoints import suspended
from test_workflow_input_tree import caller_owned_nested_question, owned_workflow_scope  # noqa: F401
from test_workflow_run_node_recovery import store  # noqa: F401


@pytest.mark.parametrize('winner', ['caller', 'monitor'])
def test_exact_question_winner_is_durable_before_launch(store, tmp_path, monkeypatch, winner):
    root, parent, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('alive', 'start_time_match'))
    launches = []
    def launched(storage, run_id):
        receipt = storage.get_run(root['workflow_run_id'])['interaction_history'][-1]
        assert receipt['answer'] == 'exact answer\nwith spaces'
        assert receipt['answered_by'] == winner
        launches.append(run_id)
    monkeypatch.setattr(workflows, '_launch', launched)
    accepted = store.control(root['workflow_run_id'], 'resume', 'exact answer\nwith spaces', decision_id='nested', interaction_owner=winner)
    receipt = accepted['interaction_history'][-1]
    assert receipt['workflow_run_id'] == child['workflow_run_id']
    assert accepted['interaction_owner'] == 'caller'
    assert interactions.projection(store, accepted)['interaction_state']['responders'] == ['caller', 'monitor']
    restarted = workflows.WorkflowStore(root=store.root)
    for responder in ('caller', 'monitor'):
        with pytest.raises(interactions.InteractionAlreadyAnswered) as loser:
            restarted.control(root['workflow_run_id'], 'resume', 'replacement', decision_id='nested', interaction_owner=responder)
        assert loser.value.data == {'code': 'interaction_already_answered', 'interaction': receipt}
    assert launches == [root['workflow_run_id']]


def test_question_race_reports_winner_to_loser(store, tmp_path, monkeypatch):
    root, _, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('alive', 'start_time_match'))
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    barrier = Barrier(2)
    def answer(responder):
        barrier.wait(timeout=5)
        try:
            return store.control(root['workflow_run_id'], 'resume', responder, decision_id='nested', interaction_owner=responder)
        except interactions.InteractionAlreadyAnswered as exc:
            return exc
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(answer, ['caller', 'monitor']))
    success = next(result for result in results if isinstance(result, dict))
    failure = next(result for result in results if isinstance(result, interactions.InteractionAlreadyAnswered))
    assert failure.data['interaction'] == success['interaction_history'][-1]
    assert store.get_run(child['workflow_run_id'])['instructions'] == failure.data['interaction']['answer']


@pytest.mark.parametrize('winner', ['caller', 'monitor'])
def test_attention_receipt_precedes_stale_checkpoint_refusal(store, tmp_path, monkeypatch, winner):
    run = suspended(store, tmp_path)
    run = store.update_run(run['workflow_run_id'], lambda r: r.update(interaction_caller={
        'kind': 'external_parent', 'identity': {'pid': 1234, 'start_time': 'qualified', 'markers': []}}), 'fixture')
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('alive', 'start_time_match'))
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    checkpoint = workflow_attention.checkpoint_id(store, run)
    result = store.control(run['workflow_run_id'], 'resume', 'continue', attention_checkpoint_id=checkpoint, interaction_owner=winner)
    with pytest.raises(interactions.InteractionAlreadyAnswered) as loser:
        store.control(run['workflow_run_id'], 'resume', 'other', attention_checkpoint_id=checkpoint, interaction_owner='monitor' if winner == 'caller' else 'caller')
    assert loser.value.data['interaction'] == result['interaction_history'][-1]


def test_exact_internal_delivery_replays_after_public_answer(store, tmp_path, monkeypatch):
    root, _, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('alive', 'start_time_match'))
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    accepted = store.control(root['workflow_run_id'], 'resume', 'answer', decision_id='nested', interaction_owner='monitor')
    source = store.get_run(child['workflow_run_id'])
    delivery = source['accepted_inputs']['nested']['delivery']
    replay = store.control(child['workflow_run_id'], 'resume', 'answer', decision_id='nested', interaction_owner='monitor', _internal=True, _delivery=delivery)
    assert replay['interaction_history'][-1] == accepted['interaction_history'][-1]
    assert replay['updated_at'] == source['updated_at']


def test_child_commit_survives_crash_before_root_receipt(store, tmp_path, monkeypatch):
    root, _, child = caller_owned_nested_question(store, tmp_path)
    monkeypatch.setattr(identity, 'check_detail', lambda _: ('alive', 'start_time_match'))
    launches = []
    monkeypatch.setattr(workflows, '_launch', lambda _, run_id: launches.append(run_id))
    update = store.update_run
    def crash(run_id, mutate, event, *args, **kwargs):
        if run_id == root['workflow_run_id'] and event == 'forwarded_input_answered':
            raise OSError('simulated interruption after source commit')
        return update(run_id, mutate, event, *args, **kwargs)
    monkeypatch.setattr(store, 'update_run', crash)
    with pytest.raises(OSError, match='simulated interruption'):
        store.control(root['workflow_run_id'], 'resume', 'durable source answer', decision_id='nested', interaction_owner='monitor')
    source = store.get_run(child['workflow_run_id'])
    winner = source['interaction_history'][-1]
    assert winner['answered_by'] == 'monitor'
    assert not store.get_run(root['workflow_run_id']).get('interaction_history')
    restarted = workflows.WorkflowStore(root=store.root)
    with pytest.raises(interactions.InteractionAlreadyAnswered) as refused:
        restarted.control(root['workflow_run_id'], 'resume', 'other answer', decision_id='nested', interaction_owner='caller')
    assert refused.value.data['interaction'] == winner
    assert restarted.get_run(child['workflow_run_id'])['instructions'] == 'durable source answer'
    recovered = restarted.get_run(root['workflow_run_id'])
    assert recovered['status'] == 'running'
    assert recovered['interaction_history'][-1] == winner
    assert recovered.get('forwarded_delivery') is None
    assert restarted.get_run(child['workflow_run_id'])['updated_at'] == source['updated_at']
    assert launches == [root['workflow_run_id']]
    with pytest.raises(interactions.InteractionAlreadyAnswered):
        restarted.control(root['workflow_run_id'], 'resume', 'third answer', decision_id='nested', interaction_owner='caller')
    assert launches == [root['workflow_run_id']]


@pytest.mark.parametrize('damage', ['missing', 'extra', 'duplicate', 'over_budget', 'invalid_responder'])
def test_answer_history_is_strict_and_bounded(store, tmp_path, damage):
    from harflow_mcp import workflow_run_documents
    run = suspended(store, tmp_path)
    receipt = {'interaction_id': 'checkpoint', 'action': 'resume', 'answer': 'exact answer',
               'answered_by': 'caller', 'answered_at': 1.0, 'workflow_run_id': run['workflow_run_id']}
    history = [receipt.copy()]
    if damage == 'missing':
        history[0].pop('answer')
    elif damage == 'extra':
        history[0]['unqualified_authority'] = True
    elif damage == 'duplicate':
        history.append(receipt.copy())
    elif damage == 'over_budget':
        history = [{**receipt, 'interaction_id': str(n)} for n in range(101)]
    else:
        history[0]['answered_by'] = 'worker'
    run['interaction_history'] = history
    with pytest.raises(workflow_run_documents.WorkflowRunDocumentError):
        workflow_run_documents.encode(run)


def test_large_unicode_answer_history_stays_readable_with_exact_latest_receipt(store, tmp_path):
    import json
    run = suspended(store, tmp_path)
    answer = "多" * (64 * 1024 // 3)
    def seed(value):
        for number in range(100):
            interactions.record_answer(value, str(number), 'resume', answer, 'caller')
    result = store.update_run(run['workflow_run_id'], seed, 'fixture')
    assert len(json.dumps(result['interaction_history'], ensure_ascii=False).encode()) <= interactions.HISTORY_BYTES
    assert result['interaction_history_truncated'] is True
    assert result['interaction_history'][-1]['answer'] == answer
    assert not result['interaction_history'][-1].get('answer_truncated')
    assert all(item['answer_truncated'] for item in result['interaction_history'][:-1])
    reread = store.get_run(run['workflow_run_id'], refresh_atomic=True)
    assert reread['interaction_history'] == result['interaction_history']


def test_oversized_answer_refuses_before_any_checkpoint_effect(store, tmp_path):
    run = suspended(store, tmp_path)
    before = store.get_run(run['workflow_run_id'], refresh_atomic=True)
    with pytest.raises(workflows.WorkflowError, match='64 KiB'):
        store.control(run['workflow_run_id'], 'resume', 'x' * (64 * 1024 + 1),
                      interaction_owner='caller')
    assert store.get_run(run['workflow_run_id'], refresh_atomic=True) == before


def test_old_attention_receipt_cannot_acknowledge_new_forwarded_token(store, tmp_path, monkeypatch):
    run = suspended(store, tmp_path)
    monkeypatch.setattr(workflows, '_launch', lambda *args: None)
    old_token = workflow_attention.checkpoint_id(store, run)
    store.control(run['workflow_run_id'], 'resume', 'continue', attention_checkpoint_id=old_token)
    def new_checkpoint(value):
        value.update(status='paused', forwarded_delivery={'id': 'new-delivery', 'payload': {
            'source_id': run['workflow_run_id'], 'action': 'resume', 'instructions': 'continue',
            'additional_attempts': 0, 'decision_id': None, 'allow_optional_review_skip': False,
            'interaction_owner': None, 'attention_checkpoint_id': 'attention:different',
            'source': {'workflow_run_id': run['workflow_run_id'], 'workflow_name': run['name']}}})
    current = store.update_run(run['workflow_run_id'], new_checkpoint, 'fixture')
    with pytest.raises(interactions.InteractionAlreadyAnswered):
        store.control(run['workflow_run_id'], 'resume', 'continue', attention_checkpoint_id=old_token)
    assert store.get_run(run['workflow_run_id']) == current
