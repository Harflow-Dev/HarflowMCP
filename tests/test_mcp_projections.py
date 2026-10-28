"""Compact MCP polling retains reports and operational evidence without static bulk."""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from harflow_mcp import mcp_projections, server


@pytest.mark.parametrize('size', [0, 100, 100000])
def test_report_is_complete_and_overhead_independent_of_static_bulk(size):
    summary = '界 final report\n' * size
    payload = {
        'task_id': 'example', 'status': 'failed', 'summary': summary,
        'enforcement': {'instructions': 'x' * 1000000}, 'mcp_servers': ['x'] * 10000,
        'failure_diagnostic': {'category': 'usage_limit', 'reason': 'provider quota'},
        'approval_summary': {'pending_count': 1, 'interaction_state': 'uncertain'},
        'supervision': {'owner_state': 'undecidable', 'capture_state': 'unknown',
                        'live_input_state': 'closed_with_owner', 'dispatch_origin_state': 'dead'},
        'outcome': {'new_commits': ['a' * 40] * 10000, 'committed_diff_stat': 'x' * 1000000,
                    'base_commit': 'a' * 40, 'head_commit': 'b' * 40,
                    'last_build_test': {'command': 'pytest', 'exit_code': 0, 'ok': True},
                    'terminal': {'head': 'b' * 40, 'observation_status': 'partial'}},
        'recovered': True, 'note': 'Exit was never observed', 'outcome_observed': False,
        'stop_reason': 'observation_unavailable', 'polling_guidance': 'inspect status',
        'artifact_summary': {'state': 'unavailable', 'count': 0},
    }
    compact = mcp_projections.task_status(payload)
    assert compact['summary'] == summary
    for name in ('failure_diagnostic', 'approval_summary', 'note', 'outcome_observed',
                 'artifact_summary', 'stop_reason', 'polling_guidance'):
        assert compact[name] == payload[name]
    assert compact['supervision'] == {k: v for k, v in payload['supervision'].items()
                                      if k != 'dispatch_origin_state'}
    assert compact['outcome']['last_build_test'] == {'command': 'pytest', 'exit_code': 0, 'ok': True}
    assert compact['outcome']['terminal']['observation_status'] == 'partial'
    assert len(json.dumps(compact).encode()) - len(json.dumps(summary).encode()) < 3000
    assert 'committed_diff_stat' not in compact['outcome']
    assert compact['detail_refs']['status']['detail'] == 'full'
    assert payload['summary'] == summary and 'enforcement' in payload


@pytest.mark.parametrize('brief', [False, True])
@pytest.mark.parametrize('include_tail', [False, True])
async def test_status_and_wait_detail_modes(monkeypatch, tmp_path, brief, include_tail):
    payload = {'task_id': 'example', 'status': 'completed', 'summary': 'final\n' * 10000,
               'enforcement': {'static': True}, 'last_output_tail': ['raw output']}
    task = SimpleNamespace(snapshot=lambda: dict(payload), finished=True)
    monkeypatch.setattr(server, '_guard_task_read', AsyncMock())
    monkeypatch.setattr(server, '_reg', lambda: SimpleNamespace(get=lambda _: task, log_dir=tmp_path))
    monkeypatch.setattr(server, '_await_with_progress', AsyncMock(return_value='terminal'))
    def shape(snapshot, **kwargs):
        return snapshot if kwargs['include_tail'] else {k: v for k, v in snapshot.items() if k != 'last_output_tail'}
    monkeypatch.setattr(server, '_status_payload', shape)
    status = await server.get_task_status('example', brief=brief, include_tail=include_tail)
    wait = await server.wait_for_task('example', timeout_seconds=0, include_tail=include_tail)
    for result in (status, wait):
        assert result['summary'] == payload['summary']
        assert 'enforcement' not in result
        assert ('last_output_tail' in result) is include_tail
    assert wait['stop_reason'] == 'terminal'
    full = await server.get_task_status('example', detail='full')
    assert full['enforcement'] == payload['enforcement']
    full_wait = await server.wait_for_task('example', timeout_seconds=0, detail='full')
    assert full_wait['enforcement'] == payload['enforcement']
    alias = await server.get_task_status('example', detail='full', brief=True)
    assert 'enforcement' not in alias and alias['summary'] == payload['summary']


async def test_workflow_discovery_compact_full_and_single_read(monkeypatch):
    definition = {'name': 'review', 'description': 'Review code', 'revision': 4,
                  'workflow_id': 'workflow-stable', 'nodes': [{'instructions': 'x' * 100000}],
                  'connections': [], 'orchestrator': {'instructions': 'purpose'}}
    call = AsyncMock(side_effect=lambda action, **kwargs: [definition] if action == 'list' else definition)
    monkeypatch.setattr(server, '_workflow_call', call)
    assert await server.list_workflows() == [{key: definition[key] for key in
        ('name', 'description', 'revision', 'workflow_id')}]
    assert await server.list_workflows(detail='full') == [definition]
    assert await server.get_workflow('review') == definition
    assert 'instructions' in definition['nodes'][0]


async def test_invalid_projection_detail_refuses_before_reads():
    for method, args in ((server.get_task_status, ('example',)),
                         (server.wait_for_task, ('example',)), (server.list_workflows, ())):
        with pytest.raises(Exception, match='detail must be compact or full'):
            await method(*args, detail='unknown')
