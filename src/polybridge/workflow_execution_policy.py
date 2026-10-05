"""Execution budgets and best-effort serial session continuity."""
from typing import Any


def visit_id(run: dict[str, Any], token: dict[str, Any]) -> str | None:
    source_id = token.get('retry_of_execution_id')
    if source_id:
        source = next((a for a in run['activations'] if a['id'] == source_id), None)
        if source:
            return source.get('visit_id') or (source.get('token') or {}).get('id')
    return token.get('visit_id') or token.get('id')


def attempts_used(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any]) -> int:
    visit = visit_id(run, token)
    return sum(a['role'] == 'node' and a['node_id'] == node['id'] and any(t.get('status') != 'not_started' for t in a.get('tasks', [])) and (run.get('execution_policy') != 'visit' or (a.get('visit_id') or (a.get('token') or {}).get('id')) == visit) for a in run['activations'])


def continuation_session_token(token: dict[str, Any], choice: dict[str, Any]) -> dict[str, Any]:
    """A retry marker belongs to its execution, never a later serial destination."""
    result = dict(token)
    if choice.get('execution_id'):
        result['retry_of_execution_id'] = choice['execution_id']
    elif choice['kind'] != 'execute':
        result.pop('retry_of_execution_id', None)
        result['via'] = choice['continuation_id']
    return result


def previous_session(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any], *, root=None) -> list[dict[str, Any]]:
    from . import workflow_delegation as d, workflows as w
    # A retained execution's own session wins for retries; predecessors are only for first entry.
    if token.get('retry_of_execution_id') or any(e['id'] == token.get('via') and e.get('backward') and e['target'] == node['id'] for e in run['definition']['connections']):
        return d.available_sessions(run, node, root=root)
    incoming = [e for e in run['definition']['connections'] if e['target'] == node['id'] and not e.get('backward')]
    refs = [token['execution_activation_id']] if token.get('execution_complete') and token.get('execution_activation_id') and token.get('node_id') != node['id'] else token.get('input_result_refs', [])
    if len(incoming) != 1 or len(refs) != 1:
        return []
    source = next((a for a in run['activations'] if a['id'] == refs[0]), None)
    if not source or source['node_id'] != incoming[0]['source'] or not d.settled(source):
        return []
    predecessor = next(n for n in run['definition']['nodes'] if n['id'] == source['node_id'])
    if predecessor['type'] != 'agent':
        return []
    # Match only the primary. A fallback must bootstrap Fresh, never inherit another candidate.
    key = w._candidate_key(node['agent'])
    primary = {**node, 'id': predecessor['id'], 'agent': {**node['agent'], 'fallbacks': []}}
    return [s for s in d.available_sessions({**run, 'activations': [{**source, 'tasks': source.get('tasks', [])[-1:]}]}, primary, root=root) if w._candidate_key(s['candidate']) == key]


def previous_session_reason(run, node, token, *, root=None):
    """Explain best-effort reuse using the actual settled source, never guessed defaults."""
    from . import workflows as w
    if previous_session(run, node, token, root=root):
        own = token.get('retry_of_execution_id') or any(e['id'] == token.get('via') and e.get('backward') for e in run['definition']['connections'])
        return 'Resumed last node session' if own else 'Resumed previous node'
    refs = [token['execution_activation_id']] if token.get('execution_complete') and token.get('execution_activation_id') else token.get('input_result_refs', [])
    if token.get('retry_of_execution_id'):
        refs = [token['retry_of_execution_id']]
    source = next((a for a in reversed(run['activations']) if a['id'] in refs and a['role'] == 'node'), None) if len(refs) == 1 else None
    if source and source.get('tasks'):
        task = source['tasks'][-1]
        differences = [key + ' differs' for key in ('backend', 'model', 'reasoning_effort', 'max_turns') if task.get('candidate', {}).get(key) != node['agent'].get(key)]
        network = False if run.get('network') is False else node.get('network', run.get('network'))
        differences += [label + ' differs' for label, actual, expected in [('freedom', task.get('freedom'), w.run_effective_freedom(run, node)), ('network', task.get('network'), network), ('repository', task.get('repo_path'), run['repo_path'])] if actual != expected]
        if differences:
            return 'Started Fresh: ' + ', '.join(differences)
    return 'Started Fresh: compatible previous session unavailable'
