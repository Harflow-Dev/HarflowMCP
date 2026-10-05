"""Applicability selection freezes whole branch generations before dispatch."""
import asyncio
import copy

import pytest

from polybridge import workflow_delegation as d
from polybridge import workflows as w
from test_workflow_delegation import Registry, run_flow, storage
from test_workflow_explicit_parallel import explicit
from test_workflow_run_node_execution import TreeRegistry, assignment_for, child_graph, parent_graph, run_tree, _start_tree, _wait_until


def selectable(nested=False):
    graph = explicit(nested)
    next(n for n in graph['nodes'] if n['id'] == 'split').update(branch_selection='orchestrator', selection_guidance='Choose applicable platforms')
    if nested:
        next(n for n in graph['nodes'] if n['id'] == 'inner').update(branch_selection='orchestrator')
    return graph


def choose_policy(selected=('split-left',), nested_selected=('inner-inner_a',), mutate=None):
    def policy(context, registry):
        entries = []
        choices = [c for c in context['valid_continuations'] if c['kind'] != 'retry_execution']
        def assign(choice):
            entry = assignment_for(choice)
            if 'branch_continuations' in choice:
                ids = nested_selected if choice['node_id'] == 'inner' else selected
                entry['branch_assignments'] = [assign(c) for c in choice['branch_continuations'] if c['continuation_id'] in ids]
                if choice.get('branch_selection') == 'orchestrator':
                    entry['selection_reason'] = 'Selected requested platform branches; other platforms are outside this request'
            return entry
        entries = [assign(c) for c in choices]
        value = {'decision_id': context['decision_id'], 'action': 'continue' if entries else 'complete', 'reason': 'Selected applicable work and excluded unrelated branches', 'next': entries}
        if mutate:
            mutate(value, context)
        return value
    return policy


def groups(run):
    return sorted(run.get('released_parallel_groups', {}).values(), key=lambda g: g.get('selection_sequence', 0))


@pytest.mark.parametrize('selected', [('split-left',), ('split-right',), ('split-left', 'split-right')])
async def test_subset_and_singleton_converge_with_recorded_assignments(storage, tmp_path, selected):
    run, registry = await run_flow(storage, tmp_path, selectable(), Registry(storage.root, choose_policy(selected)), guided=True)
    assert run['status'] == 'completed', run.get('attention_reason')
    workers = [a for a in run['activations'] if a['role'] == 'node']
    expected = ({'left', 'left2'} if 'split-left' in selected else set()) | ({'right'} if 'split-right' in selected else set())
    assert {a['node_id'] for a in workers} == expected
    group = groups(run)[0]
    assert group['expected'] == len(selected)
    assert group['selected_connection_ids'] == list(selected)
    assert group['excluded_connection_ids'] == [i for i in ('split-left', 'split-right') if i not in selected]
    assert group['selection_reason'] and group['selection_decision_id']
    assert set(group['branch_assignments']) == set(selected)
    assert len({a['assignment_prompt'] for a in workers}) == len(workers)


@pytest.mark.parametrize('invalid', ['empty', 'unknown', 'duplicate', 'missing_prompt', 'missing_reason', 'blank_reason'])
async def test_invalid_selection_dispatches_nothing(storage, tmp_path, invalid):
    def mutate(value, context):
        if context['current_stage']['node_id'] != 'start':
            return
        entry = value['next'][0]
        branches = entry['branch_assignments']
        if invalid == 'empty': entry['branch_assignments'] = []
        elif invalid == 'unknown': branches[0]['continuation_id'] = 'unknown'
        elif invalid == 'duplicate': branches.append(copy.deepcopy(branches[0]))
        elif invalid == 'missing_prompt': branches[0].pop('prompt')
        elif invalid == 'missing_reason': entry.pop('selection_reason')
        elif invalid == 'blank_reason': entry['selection_reason'] = '  '
    run, _ = await run_flow(storage, tmp_path, selectable(), Registry(storage.root, choose_policy(mutate=mutate)), guided=True)
    assert run['status'] == 'needs_attention'
    assert not any(a['role'] == 'node' for a in run['activations'])
    assert run['transitions'] == 0 and not run['joins']


@pytest.mark.parametrize('value', [None, True, [], {}, 'invalid'])
def test_branch_selection_validation_reports_workflow_error(value):
    graph = selectable()
    next(n for n in graph['nodes'] if n['id'] == 'split')['branch_selection'] = value
    with pytest.raises(w.WorkflowError, match='branch_selection'):
        w.validate_definition(graph)
    with pytest.raises(w.WorkflowError, match='branch_selection'):
        w.validate_builder_preview(graph, 'preview')


async def test_optional_only_selection_is_rejected_before_dispatch(storage, tmp_path):
    graph = selectable()
    next(n for n in graph['nodes'] if n['id'] == 'right')['optional'] = True
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, choose_policy(('split-right',))), guided=True)
    assert run['status'] == 'needs_attention'
    assert not any(a['role'] == 'node' for a in run['activations'])


async def test_selected_optional_failure_retains_required_sibling_safety(storage, tmp_path):
    graph = selectable()
    next(n for n in graph['nodes'] if n['id'] == 'right')['optional'] = True
    outputs = {'right': {'status': 'failed', 'result': {'summary': 'Definitive optional failure'}, 'evidence': []}}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, choose_policy(('split-left', 'split-right')), outputs), guided=True)
    assert run['status'] == 'completed', run.get('attention_reason')
    assert next(a for a in run['activations'] if a['node_id'] == 'right' and a['role'] == 'node')['optional_failure']
    assert groups(run)[0]['branch_states']['split-right'] == 'optional_skipped'


async def test_nested_structural_singleton_keeps_ancestor_generation(storage, tmp_path):
    graph = selectable(nested=True)
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, choose_policy(('split-right',))), guided=True)
    assert run['status'] == 'completed', run.get('attention_reason')
    history = run['released_parallel_groups']
    outer_id, outer = next((i, g) for i, g in history.items() if g['split_id'] == 'split')
    inner = next(g for g in history.values() if g['split_id'] == 'inner')
    assert inner['stack'] == [outer_id]
    assert inner['selected_connection_ids'] == ['inner-inner_a']
    assert inner['excluded_connection_ids'] == ['inner-inner_b']
    workers = {a['node_id'] for a in run['activations'] if a['role'] == 'node'}
    assert workers == {'right', 'inner_a'}


async def test_direct_structural_branch_selects_nested_entries(storage, tmp_path):
    graph = selectable(nested=True)
    graph['nodes'] = [n for n in graph['nodes'] if n['id'] != 'right']
    graph['connections'] = [e for e in graph['connections'] if e['source'] != 'right']
    next(e for e in graph['connections'] if e['id'] == 'split-right')['target'] = 'inner'
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, choose_policy(('split-right',))), guided=True)
    assert run['status'] == 'completed', run.get('attention_reason')
    assert {a['node_id'] for a in run['activations'] if a['role'] == 'node'} == {'inner_a'}
    assert len(groups(run)) == 2


async def test_loop_reentry_selects_new_membership_and_retains_old_generation(storage, tmp_path):
    graph = selectable()
    graph['connections'].append({'id': 'repeat', 'source': 'merge', 'target': 'split', 'max_retries': 1, 'condition': 'Repeat with another platform'})
    visits = 0
    def policy(context, registry):
        nonlocal visits
        selected = ('split-left',) if visits == 0 else ('split-right',)
        if context['current_stage']['node_id'] == 'merge':
            visits += 1
            chosen = 'repeat' if visits == 1 else 'merge-end'
            context = {**context, 'valid_continuations': [c for c in context['valid_continuations'] if c['continuation_id'] == chosen]}
            selected = ('split-right',)
        return choose_policy(selected)(context, registry)
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, policy), guided=True)
    assert run['status'] == 'completed', run.get('attention_reason')
    history = groups(run)
    assert len(history) == 2
    assert history[0]['selected_connection_ids'] == ['split-left']
    assert history[1]['selected_connection_ids'] == ['split-right']
    assert history[0]['selection_sequence'] < history[1]['selection_sequence']
    assert run['retry_counts']['repeat'] == 1


async def test_restart_preserves_accepted_selection_before_split_dispatch(storage, tmp_path):
    definition = w.validate_definition(selectable())
    run = storage.create_run(definition, 'Only left applies', tmp_path)
    registry = Registry(storage.root, choose_policy(('split-left',)))
    class PauseBeforeSplit(w.WorkflowSupervisor):
        async def _node(self, token):
            if token['node_id'] == 'split':
                self.update(lambda r: r.update(status='paused'), 'test_pause_before_split')
                return
            await super()._node(token)
    await asyncio.wait_for(PauseBeforeSplit(registry, storage).execute(run['workflow_run_id']), 10)
    suspended = storage.get_run(run['workflow_run_id'])
    token = suspended['pending'][0]
    assert token['node_id'] == 'split' and token['selected_connections'] == ['split-left']
    frozen_assignments = copy.deepcopy(token['assignments'])
    assert not suspended['joins']
    assert not any(a['role'] == 'node' for a in suspended['activations'])
    # A different policy on the restarted supervisor cannot reinterpret the
    # accepted split selection or its focused assignments.
    registry.policy = choose_policy(('split-right',))
    storage.control(run['workflow_run_id'], 'resume')
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 10)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    group = groups(final)[0]
    assert group['selected_connection_ids'] == ['split-left']
    assert group['branch_assignments'] == frozen_assignments
    assert {a['node_id'] for a in final['activations'] if a['role'] == 'node'} == {'left', 'left2'}


async def test_workflow_branch_selection_creates_only_selected_child(storage, tmp_path):
    child = storage.save('child', child_graph('child'))
    graph = parent_graph(child['workflow_id'], branches=2)
    next(n for n in graph['nodes'] if n['id'] == 'split')['branch_selection'] = 'orchestrator'
    storage.save('parent', graph)
    registry = TreeRegistry(storage.root, policy=choose_policy(('c1',)))
    run, _ = await run_tree(storage, tmp_path, 'parent', registry)
    assert run['status'] == 'completed', run.get('attention_reason')
    children = [r for r in storage.list_runs() if r.get('parent_link')]
    assert len(children) == 1 and children[0]['parent_link']['node_id'] == 'call2'
    assert groups(run)[0]['selected_connection_ids'] == ['c1']
    assert groups(run)[0]['excluded_connection_ids'] == ['c0']


async def test_selected_required_failure_cannot_release_singleton_barrier(storage, tmp_path):
    failure = {'status': 'failed', 'result': {'summary': 'Required branch failed'}, 'evidence': []}
    run, _ = await run_flow(storage, tmp_path, selectable(), Registry(storage.root, choose_policy(('split-right',)), {'right': failure}), guided=True)
    assert run['status'] != 'completed'
    assert len(run['joins']) == 1 and not run.get('released_parallel_groups')
    group = next(iter(run['joins'].values()))
    assert group['selected_connection_ids'] == ['split-right']
    assert group['expected'] == 1 and not group['arrived']


async def test_selectable_runtime_rejects_unguided_historical_runner(storage, tmp_path):
    run = storage.create_run(w.validate_definition(selectable()), 'Only applicable branches', tmp_path)
    storage.update_run(run['workflow_run_id'], lambda r: r.pop('runner_policy'), 'test_historical_policy')
    registry = Registry(storage.root)
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 10)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'needs_attention'
    assert not registry.calls


def test_builder_round_trip_preserves_selection_settings():
    graph = selectable()
    preview = w.validate_builder_preview(graph, 'preview')
    definition = w.validate_definition(preview)
    split = next(n for n in definition['nodes'] if n['id'] == 'split')
    assert split['branch_selection'] == 'orchestrator'
    assert split['selection_guidance'] == 'Choose applicable platforms'


async def test_required_branch_retry_recovers_inside_frozen_singleton(storage, tmp_path):
    attempts = 0
    def output(prompt, kwargs):
        nonlocal attempts
        attempts += 1
        return {'status': 'failed' if attempts == 1 else 'succeeded', 'result': {'summary': 'Observed attempt'}, 'evidence': []}
    base = choose_policy(('split-right',))
    def policy(context, registry):
        retry = next((c for c in context['valid_continuations'] if c['kind'] == 'retry_execution'), None)
        if context['current_stage']['node_id'] == 'right' and retry:
            return {'decision_id': context['decision_id'], 'action': 'continue', 'reason': 'Recover only the selected branch', 'next': [assignment_for(retry)]}
        return base(context, registry)
    run, _ = await run_flow(storage, tmp_path, selectable(), Registry(storage.root, policy, {'right': output}), guided=True)
    assert run['status'] == 'completed', run.get('attention_reason')
    assert attempts == 2
    assert len(groups(run)) == 1
    assert groups(run)[0]['selected_connection_ids'] == ['split-right']
    assert {a['node_id'] for a in run['activations'] if a['role'] == 'node'} == {'right'}


@pytest.mark.parametrize('category', ['permission', 'authority', 'uncertain'])
async def test_selection_cannot_bypass_unsafe_optional_failure(storage, tmp_path, category):
    graph = selectable()
    next(n for n in graph['nodes'] if n['id'] == 'right')['optional'] = True
    output = {'status': 'blocked', 'result': {'blocker_category': category, 'summary': 'Cannot settle safely'}, 'evidence': []}
    run, _ = await run_flow(storage, tmp_path, graph, Registry(storage.root, choose_policy(('split-left', 'split-right')), {'right': output}), guided=True)
    assert run['status'] != 'completed'
    assert not next(a for a in run['activations'] if a['role'] == 'node' and a['node_id'] == 'right').get('optional_failure')
    assert len(run['joins']) == 1


async def test_restart_after_group_reservation_keeps_identity_and_membership(storage, tmp_path):
    run = storage.create_run(w.validate_definition(selectable()), 'Only right applies', tmp_path)
    registry = Registry(storage.root, choose_policy(('split-right',)))
    class PauseBeforeWorker(w.WorkflowSupervisor):
        async def _node(self, token):
            if token['node_id'] == 'right':
                self.update(lambda r: r.update(status='paused'), 'test_pause_reserved_group')
                return
            await super()._node(token)
    await asyncio.wait_for(PauseBeforeWorker(registry, storage).execute(run['workflow_run_id']), 10)
    suspended = storage.get_run(run['workflow_run_id'])
    assert len(suspended['joins']) == 1
    generation, frozen = next(iter(suspended['joins'].items()))
    assert frozen['selected_connection_ids'] == ['split-right']
    assert suspended['pending'][0]['stack'] == [generation]
    assert not any(a['role'] == 'node' for a in suspended['activations'])
    registry.policy = choose_policy(('split-left',))
    storage.control(run['workflow_run_id'], 'resume')
    await asyncio.wait_for(w.WorkflowSupervisor(registry, storage).execute(run['workflow_run_id']), 10)
    final = storage.get_run(run['workflow_run_id'])
    assert final['status'] == 'completed', final.get('attention_reason')
    assert list(final['released_parallel_groups']) == [generation]
    recorded = final['released_parallel_groups'][generation]
    for key in ('selected_connection_ids', 'excluded_connection_ids', 'branch_assignments', 'selection_reason', 'selection_decision_id', 'selection_sequence'):
        assert recorded[key] == frozen[key]
    assert {a['node_id'] for a in final['activations'] if a['role'] == 'node'} == {'right'}


@pytest.mark.parametrize('branch_id', ['split-right', 'unknown'])
def test_excluded_or_unknown_arrival_cannot_mutate_frozen_barrier(storage, tmp_path, branch_id):
    run = storage.create_run(w.validate_definition(selectable()), 'Only left applies', tmp_path)
    token = {'id': 'arrival', 'node_id': 'merge', 'stack': ['generation'], 'branch_ids': {'generation': branch_id}, 'context': {}, 'failed_execution_refs': ['failed-execution']}
    def seed(r):
        r.update(status='running', pending=[token])
        r['activations'].append({'id': 'failed-execution', 'tasks': [], 'node_result': {'status': 'failed'}})
        r['joins']['generation'] = {'join_id': 'merge', 'split_id': 'split', 'stack': [], 'expected': 1, 'arrived': [], 'selected_connection_ids': ['split-left'], 'excluded_connection_ids': ['split-right'], 'branch_ids': ['split-left']}
    storage.update_run(run['workflow_run_id'], seed, 'test_frozen_barrier')
    before = storage.get_run(run['workflow_run_id'])
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = run['workflow_run_id']
    with pytest.raises(w.WorkflowError, match='frozen selected'):
        supervisor._arrive_join(token)
    after = storage.get_run(run['workflow_run_id'])
    assert after['status'] == before['status']
    assert after['joins'] == before['joins'] and after['pending'] == before['pending']
    assert not after.get('released_parallel_groups')


def test_missing_branch_identity_cannot_use_token_id_as_selected_membership(storage, tmp_path):
    run = storage.create_run(w.validate_definition(selectable()), 'Only left applies', tmp_path)
    token = {'id': 'split-left', 'node_id': 'merge', 'stack': ['generation'], 'context': {}}
    def seed(r):
        r.update(status='running', pending=[token])
        r['joins']['generation'] = {'join_id': 'merge', 'split_id': 'split', 'stack': [], 'expected': 1, 'arrived': [], 'selected_connection_ids': ['split-left']}
    storage.update_run(run['workflow_run_id'], seed, 'test_missing_branch_identity')
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = run['workflow_run_id']
    with pytest.raises(w.WorkflowError, match='frozen selected'):
        supervisor._arrive_join(token)
    assert len(supervisor.run()['joins']) == 1


def test_arrival_count_cannot_release_before_all_frozen_members_arrive(storage, tmp_path):
    run = storage.create_run(w.validate_definition(selectable()), 'Both apply', tmp_path)
    token = {'id': 'arrival', 'node_id': 'merge', 'stack': ['generation'], 'branch_ids': {'generation': 'split-left'}, 'context': {}}
    def seed(r):
        r.update(status='running', pending=[token])
        r['joins']['generation'] = {'join_id': 'merge', 'split_id': 'split', 'stack': [], 'expected': 1, 'arrived': [], 'selected_connection_ids': ['split-left', 'split-right']}
    storage.update_run(run['workflow_run_id'], seed, 'test_inconsistent_expected_count')
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = run['workflow_run_id']
    assert supervisor._arrive_join(token)
    final = supervisor.run()
    assert final['joins']['generation']['arrival_ids'] == ['split-left']
    assert not final.get('released_parallel_groups')


@pytest.mark.parametrize('branch_id', ['split-right', 'unknown'])
def test_excluded_late_arrival_cannot_mutate_released_selection(storage, tmp_path, branch_id):
    run = storage.create_run(w.validate_definition(selectable()), 'Only left applies', tmp_path)
    token = {'id': 'arrival', 'node_id': 'merge', 'stack': ['generation'], 'branch_ids': {'generation': branch_id}, 'context': {}}
    def seed(r):
        r.update(status='running', pending=[token], released_parallel_groups={'generation': {'split_id': 'split', 'selected_connection_ids': ['split-left'], 'excluded_connection_ids': ['split-right']}})
    storage.update_run(run['workflow_run_id'], seed, 'test_released_selection')
    before = storage.get_run(run['workflow_run_id'])
    supervisor = w.WorkflowSupervisor(Registry(storage.root), storage)
    supervisor.run_id = run['workflow_run_id']
    with pytest.raises(w.WorkflowError, match='frozen selected'):
        supervisor._arrive_join(token)
    assert supervisor.run()['pending'] == before['pending']
    assert supervisor.run()['released_parallel_groups'] == before['released_parallel_groups']
