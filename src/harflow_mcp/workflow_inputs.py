"""Bounded, ownership-verified projection of live workflow input checkpoints."""
import time
from .catalog import Catalog
from .workflow_references import MAX_WORKFLOW_NESTING_DEPTH


def project(store, root, *, _records=None):
    from .workflows import WorkflowError
    budget = Catalog(store.runs, '.json', layout=store.state_layout)
    root_id = (root.get('parent_link') or {}).get('root_workflow_run_id') or root['workflow_run_id']
    selected_id = root['workflow_run_id']
    # Refresh root under the same shared budget as all descendants.
    root = store.get_run(selected_id, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget, refresh_atomic=True)
    stack = [(root, [selected_id])]
    seen = set()
    inputs, attention = [], []
    now = time.time()
    while stack:
        run, path = stack.pop()
        identifier = run['workflow_run_id']
        if identifier in seen:
            continue
        seen.add(identifier)
        if _records is not None:
            _records.add(identifier)
        source = run.get('input_source') or {}
        decision = run.get('input_decision_id')
        pending = next((t for t in run.get('pending', []) if t.get('decision_id') == decision), {})
        question_activation = next((a for a in run.get('activations', []) if any(q.get('decision_id') == decision for q in a.get('questions', []))), {})
        node_id = pending.get('node_id') or question_activation.get('node_id') or (run.get('parent_link') or {}).get('node_id')
        node = next((n for n in run.get('definition', {}).get('nodes', []) if n.get('id') == node_id), {})
        entry = dict(workflow_run_id=identifier, workflow_name=run.get('name', ''), node_id=node_id, node_title=node.get('title', node_id or ''), path=path)
        if run.get('status') == 'needs_input' and decision and run.get('input_question') and source.get('workflow_run_id') in (None, identifier) and decision not in run.get('accepted_inputs', {}):
            since = run.get('updated_at', run.get('created_at', now))
            inputs.append(dict(entry, decision_id=decision, question=run['input_question'], question_items=run.get('input_question_items', []), waiting_since=since, waiting_seconds=max(0, now-since), optional_review_skip_available=run.get('optional_review_skip_available') is True))
        if run.get('status') in {'needs_attention', 'paused', 'failed'} and not run.get('attention_source'):
            attention.append(dict(entry, status=run['status'], reason=run.get('attention_reason') or run.get('failure_reason') or '', waiting_since=run.get('updated_at', run.get('created_at', now)), waiting_seconds=max(0, now-run.get('updated_at', run.get('created_at', now)))))
        for activation in run.get('activations', []):
            invocation = activation.get('invocation')
            if not invocation:
                continue
            outcome = (activation.get('node_result', {}).get('result') or {}).get('child_outcome') or {}
            if activation.get('status') == 'completed' and invocation.get('stage') == 'settled' and outcome.get('kind') == 'completed':
                continue
            if len(path) >= MAX_WORKFLOW_NESTING_DEPTH:
                raise WorkflowError('Workflow input tree exceeds maximum nesting depth')
            child_id = invocation['child_workflow_run_id']
            child = store.get_run(child_id, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget, refresh_atomic=True)
            link = child.get('parent_link') or {}
            if child.get('workflow_run_id') != child_id or link.get('workflow_run_id') != identifier or link.get('execution_id') != activation.get('id') or link.get('root_workflow_run_id') != root_id:
                raise WorkflowError('Workflow input tree ownership could not be verified')
            stack.append((child, [*path, child_id]))
    if len({entry['decision_id'] for entry in inputs}) != len(inputs):
        raise WorkflowError('Workflow input decisions are ambiguous')
    inputs.sort(key=lambda e: (e['waiting_since'], e['workflow_run_id'], e['decision_id']))
    attention.sort(key=lambda e: e['workflow_run_id'])
    return dict(pending_inputs=inputs, pending_attention=attention, pending_input_count=len(inputs), pending_attention_count=len(attention))


def verified_source(store, root, identifier):
    records = set()
    project(store, root, _records=records)
    return identifier in records
