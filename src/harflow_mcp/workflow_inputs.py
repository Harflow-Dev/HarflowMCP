"""Bounded, ownership-verified projection of live workflow input checkpoints."""
import time
from .catalog import Catalog
from .workflow_references import MAX_WORKFLOW_NESTING_DEPTH


def project(store, root, *, _records=None, _cached=None, _budget=None):
    from .workflows import WorkflowError
    from .workflow_run_documents import WorkflowRunDocumentError
    from .bounded_io import ReadLimit
    budget = _budget or Catalog(store.runs, '.json', layout=store.state_layout)
    cached = _cached if _cached is not None else {}
    def read(identifier):
        if identifier not in cached:
            cached[identifier] = store.get_run(identifier, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget, refresh_atomic=True)
        return cached[identifier]
    root_id = (root.get('parent_link') or {}).get('root_workflow_run_id') or root['workflow_run_id']
    selected_id = root['workflow_run_id']
    # Refresh root under the same shared budget as all descendants.
    root = read(selected_id)
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
        recovery = next((run.get(key) for key in ('orchestrator_usage_recovery', 'worker_usage_recovery') if (run.get(key) or {}).get('decision_id') == decision), None)
        node_id = (recovery or {}).get('node_id') or pending.get('node_id') or question_activation.get('node_id') or (run.get('parent_link') or {}).get('node_id')
        node = next((n for n in run.get('definition', {}).get('nodes', []) if n.get('id') == node_id), {})
        entry = dict(workflow_run_id=identifier, workflow_name=run.get('name', ''), node_id=node_id, node_title=node.get('title', node_id or ''), path=path)
        if run.get('status') == 'needs_input' and decision and run.get('input_question') and source.get('workflow_run_id') in (None, identifier) and decision not in run.get('accepted_inputs', {}):
            since = run.get('updated_at', run.get('created_at', now))
            metadata = {}
            if recovery:
                execution = next((a for a in run.get('activations', []) if a.get('id') == recovery.get('execution_id')), {})
                evidence = (execution.get('node_result') or {}).get('evidence') or []
                task_ids = {item.get('task_id') for item in evidence if isinstance(item, dict) and item.get('task_id')}
                attempts = execution.get('tasks', [])
                worker = next((task for task in reversed(attempts) if task.get('task_id') in task_ids), None)
                if worker is None:
                    worker = next((task for task in reversed(attempts) if (task.get('result', {}).get('failure_diagnostic') or {}).get('category') == 'usage_limit'), {})
                result = worker.get('result') or {}
                configured = worker.get('candidate') or {}
                candidate = recovery.get('failed_candidate') or {key: result.get(key) or configured.get(key) for key in ('backend', 'model')}
                diagnostic = recovery.get('failure_diagnostic') or {}
                metadata = dict(kind='usage_limit', backend=candidate.get('backend') or diagnostic.get('backend'), model=candidate.get('model') or diagnostic.get('model'), resume_hint='Resume this decision_id with an offered choice; inspect the source workflow for recovery details')
            metadata = {key: value for key, value in metadata.items() if value is not None}
            inputs.append(dict(entry, **metadata, decision_id=decision, question=run['input_question'], question_items=run.get('input_question_items', []), waiting_since=since, waiting_seconds=max(0, now-since), optional_review_skip_available=run.get('optional_review_skip_available') is True))
        if run.get('status') in {'needs_attention', 'paused', 'failed'} and not run.get('attention_source'):
            attention.append(dict(entry, status=run['status'], reason=run.get('attention_reason') or run.get('failure_reason') or '', waiting_since=run.get('updated_at', run.get('created_at', now)), waiting_seconds=max(0, now-run.get('updated_at', run.get('created_at', now)))))
        for activation in run.get('activations', []):
            invocation = activation.get('invocation')
            if not invocation:
                continue
            outcome = (activation.get('node_result', {}).get('result') or {}).get('child_outcome') or {}
            if activation.get('status') == 'completed' and invocation.get('stage') == 'settled' and outcome.get('kind') == 'completed':
                continue
            if invocation.get('stage') == 'preparing':
                continue
            child_id = invocation.get('child_workflow_run_id')
            child_path = [*path, child_id] if child_id else list(path)
            try:
                if len(path) >= MAX_WORKFLOW_NESTING_DEPTH:
                    raise WorkflowError('Workflow input tree exceeds maximum nesting depth')
                child = read(child_id)
                link = child.get('parent_link') or {}
                if child.get('workflow_run_id') != child_id or link.get('workflow_run_id') != identifier or link.get('execution_id') != activation.get('id') or link.get('root_workflow_run_id') != root_id:
                    raise WorkflowError('Workflow input tree ownership could not be verified')
            except ReadLimit:
                raise
            except (OSError, ValueError, KeyError, TypeError, AttributeError, WorkflowError, WorkflowRunDocumentError) as exc:
                attention.append(dict(workflow_run_id=child_id or identifier, workflow_name='', node_id=activation.get('node_id'), node_title=activation.get('node_id') or '', parent_workflow_run_id=identifier, child_workflow_run_id=child_id, execution_id=activation.get('id'), path=child_path, status='needs_attention', reason=str(exc), waiting_since=now, waiting_seconds=0))
                continue
            stack.append((child, child_path))
    if len({entry['decision_id'] for entry in inputs}) != len(inputs):
        raise WorkflowError('Workflow input decisions are ambiguous')
    inputs.sort(key=lambda e: (e['waiting_since'], e['workflow_run_id'], e['decision_id']))
    attention.sort(key=lambda e: e['workflow_run_id'])
    return dict(pending_inputs=inputs, pending_attention=attention, pending_input_count=len(inputs), pending_attention_count=len(attention))


def verified_source(store, root, identifier):
    records = set()
    project(store, root, _records=records)
    return identifier in records
