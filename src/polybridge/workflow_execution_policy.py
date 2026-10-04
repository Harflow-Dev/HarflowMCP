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


def previous_session(run: dict[str, Any], node: dict[str, Any], token: dict[str, Any], *, root=None) -> list[dict[str, Any]]:
    from . import workflow_delegation as d, workflows as w
    # A retained execution's own session wins for retries; predecessors are only for first entry.
    if token.get('retry_of_execution_id'):
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
