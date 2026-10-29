"""Explicit bounded task projections. Full task state remains on the detail APIs."""
from __future__ import annotations

import difflib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from . import store
from .task_outcomes import project_outcome, endpoint_projection

BRIEF_BYTES = 8192
TEXT_BYTES = 1024
LIST_FIELDS = frozenset({'task_id', 'backend', 'status', 'title', 'group', 'repo_path',
    'started_at', 'finished_at', 'session_id', 'parent_task_id', 'root_task_id',
    'duration_seconds', 'background_idle_seconds', 'output_capture', 'input_channel', 'last_captured_output_at', 'recovered', 'observation_mode', 'observation_note', 'owner', 'dispatch_origin', 'responding_server', 'persisted_status', 'observed_exit',
    'needs_reconciliation', 'process_identity_state', 'needs_direct_lookup', 'note'})
REQUIRED_FIELDS = frozenset({'task_id', 'status', 'needs_reconciliation',
    'needs_direct_lookup', 'persisted_status', 'observed_exit', 'process_identity_state'})
ACCEPTED_FILTERS = ('group', 'title_contains', 'repo', 'status', 'backend', 'since', 'predecessor_task_id')


def _suggest(name: str, accepted: set[str]) -> str | None:
    if name == 'repo' and 'repo_path' in accepted:
        return 'repo_path'
    match = difflib.get_close_matches(name, sorted(accepted), n=1, cutoff=0.6)
    return match[0] if match else None


def _unknown_names(kind: str, unknown: list[str], accepted: str, *, vocabulary: set[str], accepted_label: str = 'accepted') -> str:
    parts = []
    for name in dict.fromkeys(unknown):
        hint = _suggest(name, vocabulary) if isinstance(name, str) else None
        parts.append(f'{name!r}' + (f' (did you mean {hint!r}?)' if hint else ''))
    return f'unknown {kind}: ' + ', '.join(parts) + f'; {accepted_label}: {accepted}'


def validate_filters(filters: dict[str, Any]) -> dict[str, Any]:
    accepted = ', '.join(ACCEPTED_FILTERS)
    if not isinstance(filters, dict):
        raise ValueError(f'task listing filters must be an object; accepted filters: {accepted}')
    unknown = [key for key in filters if key not in ACCEPTED_FILTERS]
    if unknown:
        raise ValueError(_unknown_names('task listing filters', unknown, accepted, vocabulary=set(ACCEPTED_FILTERS), accepted_label='accepted filters'))
    result = {}
    for key, value in filters.items():
        if value is None:
            continue
        if key == 'predecessor_task_id':
            store.validate_task_id(value)
        if key == 'group':
            if not isinstance(value, str) or not value.strip() or len(value) > 128:
                raise ValueError('group must be a nonblank string of at most 128 characters')
        elif not isinstance(value, str) or not value or len(value.encode()) > 200:
            raise ValueError(f'{key} must be a nonempty string of at most 200 UTF-8 bytes')
        if key == 'since':
            try:
                date = datetime.fromisoformat(value.replace('Z', '+00:00'))
                if date.tzinfo is None:
                    raise ValueError()
                value = date.timestamp()
            except (ValueError, OverflowError):
                raise ValueError('since must be an ISO8601 timestamp with timezone') from None
        if key == 'status' and value not in store.TERMINAL_RECORD_STATUSES | {'running', 'unknown'}:
            raise ValueError('Unknown task status filter')
        if key == 'backend':
            from .backends import get
            get(value)
        result[key] = value
    return result


def validate_fields(fields: list[str] | None) -> set[str]:
    if fields is None:
        return set(LIST_FIELDS)
    accepted = ', '.join(sorted(LIST_FIELDS))
    required = ', '.join(sorted(REQUIRED_FIELDS))
    if not isinstance(fields, list):
        raise ValueError(f'fields must be a list of compact task field names, not {type(fields).__name__}; accepted: {accepted}')
    if not fields:
        raise ValueError(
            'fields must be a nonempty list of compact task field names; '
            f'accepted: {accepted}. Identity and status fields are always included: {required}')
    if len(fields) > len(LIST_FIELDS):
        raise ValueError(
            f'fields lists {len(fields)} names, which is more than the {len(LIST_FIELDS)} accepted compact task fields; '
            f'accepted: {accepted}')
    bad = [index for index, item in enumerate(fields) if not isinstance(item, str)]
    if bad:
        shown = ', '.join(f'index {index} is {type(fields[index]).__name__}' for index in bad)
        raise ValueError(f'fields entries must be strings; {shown}')
    unknown = [item for item in fields if item not in LIST_FIELDS]
    if unknown:
        raise ValueError(
            _unknown_names('compact task fields', unknown, accepted, vocabulary=set(LIST_FIELDS))
            + f'. Identity and status fields are always included: {required}')
    return set(fields) | REQUIRED_FIELDS


def _text(value: Any, maximum: int = TEXT_BYTES) -> str:
    return str(value).encode('utf-8')[:maximum].decode('utf-8', errors='ignore')


def brief_status(payload: dict[str, Any]) -> dict[str, Any]:
    """Project an already-authorized status payload, preserving uncertainty and references.

    This is display-only truncation; stored caller prose and full APIs are untouched.
    The JSON document is capped at 8 KiB (ensure_ascii=True encoding).
    """
    keys = LIST_FIELDS | {'exit_code', 'owned_by_live_server', 'workflow_run_id', 'workflow_node_id'}
    result = {key: (_text(value, 256) if isinstance(value, str) else value)
              for key, value in payload.items() if key in keys and (value is None or isinstance(value, (str, bool, int, float)))}
    if isinstance(payload.get('supervision'), dict):
        result['supervision'] = payload['supervision']
    for key in ('owner', 'dispatch_origin', 'responding_server'):
        value = payload.get(key)
        if isinstance(value, dict) and type(value.get('pid')) is int:
            result[key] = {'pid': value['pid'], 'start_time': _text(value['start_time'], 128) if isinstance(value.get('start_time'), str) else None}
    if isinstance(payload.get('successor_tasks'), dict):
        result['successor_tasks'] = payload['successor_tasks']
    artifact = payload.get('artifact_summary')
    if isinstance(artifact, dict):
        count, state = artifact.get('count'), artifact.get('state')
        if type(count) is not int or not 0 <= count <= 100 or state not in ('available', 'unavailable'):
            count, state = 0, 'unavailable'
        projected = {'count': count, 'state': state}
        reference = artifact.get('detail_ref')
        task_id = payload.get('task_id')
        if (isinstance(task_id, str) and store.TASK_ID_PATTERN.fullmatch(task_id)
                and isinstance(reference, dict)
                and reference == {'tool': 'get_task_artifacts', 'task_id': task_id}):
            projected['detail_ref'] = {'tool': 'get_task_artifacts', 'task_id': task_id}
        result['artifact_summary'] = projected
    refs = {'status': {'tool': 'get_task_status', 'task_id': _text(payload.get('task_id'), 64)},
            'events': {'tool': 'get_task_events', 'task_id': _text(payload.get('task_id'), 64)}}
    truncated = []
    for key in ('summary', 'error', 'note'):
        if payload.get(key) is not None:
            original = payload[key] if isinstance(payload[key], str) else json.dumps(payload[key], ensure_ascii=False)
            result[key] = _text(original)
            if original != result[key]:
                truncated.append(key)
    for key, allowed in (('outcome', ('base_commit', 'head_commit', 'new_commits', 'new_commits_truncated', 'local_upstream', 'pushed', 'publication_evidence', 'attribution')), ('failure_diagnostic', ('category', 'reason', 'source', 'reset_at', 'background_idle_seconds', 'elapsed_background_wait_seconds', 'background_tasks', 'latest_result', 'successful_agent_result'))):
        value = payload.get(key)
        if key == 'outcome':
            value = project_outcome(value)
        if isinstance(value, dict):
            def compact(value, depth=0):
                if value is None or isinstance(value, (bool, int, float)):
                    return value
                if isinstance(value, str):
                    return _text(value, 160)
                if depth < 2 and isinstance(value, list):
                    return [compact(item, depth + 1) for item in value[:3]]
                if depth < 2 and isinstance(value, dict):
                    return {_text(k, 32): compact(v, depth + 1) for k, v in list(value.items())[:4]}
                return None
            result[key] = {name: compact(value[name]) for name in allowed if name in value}
            if key == 'outcome':
                result[key].update(endpoint_projection(value))
                if isinstance(value.get('new_commits'), list):
                    result[key]['new_commits_truncated'] = bool(value.get('new_commits_truncated') or len(value['new_commits']) > 3)
            if result[key] != value:
                truncated.append(key)
        elif value is not None:
            result[key] = _text(value, 160)
            truncated.append(key)
    recent = payload.get('recent_activity') or []
    result['recent_activity'] = [_text(item, 160) for item in recent[-3:]]
    result['detail_refs'] = refs
    result['truncated_fields'] = truncated
    # ASCII escaping can expand one Unicode byte to six JSON bytes.
    while len(json.dumps(result, ensure_ascii=True).encode()) > BRIEF_BYTES:
        candidates = [key for key, value in result.items() if isinstance(value, str) and len(value) > 32]
        if not candidates:
            optional = [key for key in ('recent_activity', 'outcome', 'failure_diagnostic') if key in result]
            if optional:
                key = max(optional, key=lambda k: len(json.dumps(result[k], ensure_ascii=True)))
                result.pop(key)
                if key not in truncated:
                    truncated.append(key)
                continue
            break
        key = max(candidates, key=lambda k: len(json.dumps(result[k], ensure_ascii=True)))
        result[key] = result[key][:len(result[key]) // 2]
        if key not in truncated:
            truncated.append(key)
    return result


def list_tasks(log_dir: Path, *, limit: int = 50, cursor: str | None = None,
               fields: list[str] | None = None, group: str | None = None,
               title_contains: str | None = None, repo: str | None = None,
               status: str | None = None, backend: str | None = None,
               since: str | None = None) -> dict[str, Any]:
    """Bounded indexed history; no full task snapshot or event-log replay."""
    selected = validate_fields(fields)
    filters = dict(group=group, title_contains=title_contains, repo=repo,
                   status=status, backend=backend, since=since)
    validate_filters(filters)
    result = store.list_page(log_dir, limit=limit, cursor=cursor, filters=filters)
    result['items'] = [{key: value for key, value in item.items() if key in selected}
                       for item in result['items'] if status is None or item.get('status') == status]
    # Store reconciles identities after indexed selection. Do not publish a row
    # that ceased matching during that reconciliation; the cursor still advances.
    for key in ('total_active_count', 'total_active_root_count', 'total_attention_root_count'):
        result.pop(key, None)
    result.pop('related_headers', None)
    return result
