"""MCP-only polling projections; persisted snapshots and ctl contracts stay complete."""
from __future__ import annotations

from collections import OrderedDict
from threading import Lock
from typing import Any

from .task_outcomes import endpoint_projection

_ADOPTIONS: OrderedDict[tuple, None] = OrderedDict()
_ADOPTION_LOCK = Lock()
_ADOPTION_LIMIT = 1024


def _first_adoption(payload: dict[str, Any], adoption: dict[str, Any]) -> bool:
    """Per-reader display deduplication only; never modifies execution or persisted state."""
    previous = adoption.get('previous_server') or {}
    current = adoption.get('current_server') or {}
    key = (payload.get('raw_stream_log'), payload.get('task_id'), previous.get('pid'),
           previous.get('start_time'), current.get('pid'), current.get('start_time'))
    with _ADOPTION_LOCK:
        first = key not in _ADOPTIONS
        _ADOPTIONS[key] = None
        _ADOPTIONS.move_to_end(key)
        while len(_ADOPTIONS) > _ADOPTION_LIMIT:
            _ADOPTIONS.popitem(last=False)
    return first

# Operational evidence is retained verbatim. In particular, summary is a final report,
# not a preview: its size must never determine whether the caller can read it.
STATUS_FIELDS = frozenset('''task_id backend status title session_id parent_task_id
root_task_id continuation_of_task_id chain_root_task_id workflow_run_id workflow_node_id
started_at finished_at duration_seconds summary error is_error exit_code failure_diagnostic
approval_summary interaction_owner caller_approvals caller_write_approvals recovered note
observation_mode observation_note owner responding_server persisted_status observed_exit
needs_reconciliation process_identity_state needs_direct_lookup owned_by_live_server
output_capture input_channel last_captured_output_at output_drained live_input_state
input_closed_reason pending_messages queued_followups successor_tasks artifact_summary next_step outcome_observed
launch_state not_started_reason resources notices background_idle_seconds adopted_after_restart
requested_timeout_seconds effective_timeout_seconds until timed_out stop_reason elapsed_seconds
polling_guidance'''.split())


def task_status(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep reports and operational uncertainty while omitting static execution metadata."""
    result = {key: value for key, value in payload.items() if key in STATUS_FIELDS}
    result['recent_activity'] = [str(item)[:160] for item in (payload.get('recent_activity') or [])[-3:]]
    task_id = payload.get('task_id')
    result['detail_refs'] = {
        'status': {'tool': 'get_task_status', 'task_id': task_id, 'detail': 'full'},
        'events': {'tool': 'get_task_events', 'task_id': task_id},
    }
    supervision = payload.get('supervision')
    if isinstance(supervision, dict):
        result['supervision'] = {key: value for key, value in supervision.items()
                                 if key != 'dispatch_origin_state'}
        adoption = supervision.get('adopted_after_restart')
        if isinstance(adoption, dict):
            # A dead dispatcher with a surviving owner is an observation change, not a warning.
            # Present it once per observing process, keeping full diagnostics available on demand.
            result.pop('note', None)
            if not _first_adoption(payload, adoption):
                result['supervision'].pop('adopted_after_restart', None)
                result.pop('observation_note', None)
        elif (supervision.get('dispatch_origin_state') == 'dead'
              and payload.get('status') == 'running'
              and (supervision.get('owner_state') != 'alive'
                   or supervision.get('capture_state') != 'live'
                   or supervision.get('live_input_state') in {'closed_with_owner', 'unknown'})):
            result['supervision_issue'] = (
                'The dispatch server has exited and surviving execution/capture continuity '
                'is not confirmed. Inspect owner_state, process_state, capture_state and '
                'live_input_state; full status retains dispatch diagnostics.'
            )
    recovery = payload.get('recovery_activity')
    if isinstance(recovery, dict):
        result['recovery_activity'] = {key: value for key, value in recovery.items() if key != 'events'}
    outcome = payload.get('outcome')
    if isinstance(outcome, dict):
        # Endpoint evidence is bounded independently of graph/log/report size. Detailed
        # command output, diffs and external-write arrays remain on the full read.
        result['outcome'] = endpoint_projection(outcome) | {
            key: (outcome[key][:512] if isinstance(outcome[key], str) else outcome[key])
            for key in ('base_commit', 'head_commit', 'pushed', 'attribution')
            if key in outcome}
        for phase in ('baseline', 'terminal'):
            endpoint = outcome.get(phase)
            if isinstance(endpoint, dict) and phase in result['outcome']:
                state = endpoint.get('observation_status')
                if state in ('success', 'partial', 'unavailable'):
                    result['outcome'][phase]['observation_status'] = state
        commits = outcome.get('new_commits')
        if isinstance(commits, list):
            result['outcome']['new_commits_count'] = len(commits)
            result['outcome']['new_commits_truncated'] = bool(outcome.get('new_commits_truncated'))
        for key in ('local_upstream', 'last_build_test', 'publication_evidence'):
            value = outcome.get(key)
            if isinstance(value, dict):
                result['outcome'][key] = {name: (value[name][:512] if isinstance(value[name], str) else value[name]) for name in
                    ('ref', 'ahead_behind', 'provenance', 'command', 'command_truncated',
                     'status', 'exit_code', 'ok', 'exit_code_scope', 'kind')
                    if name in value and isinstance(value[name], (str, int, bool, type(None)))}
    if 'last_output_tail' in payload:
        result['last_output_tail'] = payload['last_output_tail']
    if 'stderr_tail' in payload:
        result['stderr_tail'] = payload['stderr_tail']
    return result


def workflow_definition(payload: dict[str, Any]) -> dict[str, Any]:
    """Discovery fields sufficient to select and pin a start, without authored graph prose."""
    return {key: payload.get(key) for key in ('name', 'description', 'revision', 'workflow_id')}
