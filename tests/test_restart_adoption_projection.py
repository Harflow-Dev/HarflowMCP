"""Restart observations are display-only; lost supervision remains visible."""
from copy import deepcopy

import pytest

from harflow_mcp import mcp_projections


def restarted_payload():
    return {
        'task_id': 'adoption-task', 'raw_stream_log': '/isolated/tasks/adoption-task.jsonl',
        'status': 'running', 'summary': None,
        'dispatch_origin': {'pid': 1, 'start_time': 'old'},
        'observation_note': 'Dispatch server restarted; execution owner is still running.',
        'note': 'Recovered from disk: owned by live server',
        'supervision': {
            'owner_state': 'alive', 'process_state': 'alive', 'capture_state': 'live',
            'live_input_state': 'not_applicable', 'dispatch_origin_state': 'dead',
            'adopted_after_restart': {'at': '2026-10-28T16:58:00+00:00',
                'previous_server': {'pid': 1, 'start_time': 'old'},
                'current_server': {'pid': 2, 'start_time': 'new'}},
        },
    }


def test_adoption_once_per_observer_generation_without_mutating_full_status():
    mcp_projections._ADOPTIONS.clear()
    payload = restarted_payload()
    before = deepcopy(payload)
    first = mcp_projections.task_status(payload)
    again = mcp_projections.task_status(payload)
    assert 'adopted_after_restart' in first['supervision']
    assert 'observation_note' in first
    assert 'adopted_after_restart' not in again['supervision']
    assert 'observation_note' not in again
    assert 'note' not in first and 'note' not in again
    assert 'dispatch_origin' not in first
    assert 'dispatch_origin_state' not in first['supervision']
    assert payload == before
    payload['supervision']['adopted_after_restart']['current_server']['start_time'] = 'next'
    assert 'adopted_after_restart' in mcp_projections.task_status(payload)['supervision']


@pytest.mark.parametrize('owner,capture,input_state', [
    ('dead', 'lost_with_owner', 'closed_with_owner'),
    ('undecidable', 'unknown', 'unknown'),
    ('alive', 'live', 'unknown'),
])
def test_dead_origin_still_highlighted_when_supervision_not_confirmed(owner,capture,input_state):
    payload = restarted_payload()
    payload['supervision'].pop('adopted_after_restart')
    payload['supervision'].update(owner_state=owner, capture_state=capture, live_input_state=input_state)
    compact = mcp_projections.task_status(payload)
    assert 'not confirmed' in compact['supervision_issue']
    assert compact['supervision']['owner_state'] == owner
    assert compact['supervision']['capture_state'] == capture
    assert compact['supervision']['live_input_state'] == input_state
    assert compact['note'] == payload['note']


def test_adoption_cache_bounded(monkeypatch):
    monkeypatch.setattr(mcp_projections, '_ADOPTION_LIMIT', 2)
    mcp_projections._ADOPTIONS.clear()
    payload = restarted_payload()
    for i in range(10):
        payload['task_id'] = f'adoption-{i}'
        mcp_projections.task_status(payload)
    assert len(mcp_projections._ADOPTIONS) == 2
    mcp_projections._ADOPTIONS.clear()


def test_dispatcher_exit_is_not_a_warning_while_live_owner_drains_agent_exit():
    payload = restarted_payload()
    payload['supervision'].pop('adopted_after_restart')
    payload['supervision']['process_state'] = 'dead'
    compact = mcp_projections.task_status(payload)
    assert 'supervision_issue' not in compact
    assert compact['supervision']['process_state'] == 'dead'
