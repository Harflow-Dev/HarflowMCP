"""Authoritative quota envelopes, without model calls."""
import pytest
from polybridge.backends import get as get_backend
from polybridge.backends.base import Accumulator

@pytest.mark.parametrize('name,envelope', [('claude','error'),('codex','turn.failed'),('opencode','error'),('vibe','error'),('antigravity','result')])
def test_limit_is_sticky_after_progress_and_success(name, envelope):
    backend = get_backend(name)
    acc = Accumulator(summary='partial work', saw_final_message=True)
    error = {'code':'usage_limit_reached', 'reset_at':'2026-10-10T01:02:03Z'}
    event = {'event':'result', 'result':{'status':'ERROR','error':error}} if name == 'antigravity' else {'type':envelope,'error':error}
    backend.ingest(event, acc)
    assert acc.failure_diagnostic['category'] == 'usage_limit'
    assert acc.failure_diagnostic['reset_at'] == '2026-10-10T01:02:03Z'
    assert acc.error_result_seen
    assert backend.classify(acc, 0) == 'failed'
    first = acc.failure_diagnostic.copy()
    backend.ingest({'type':'assistant','message':{'content':[{'type':'text','text':'usage_limit_reached'}]}}, acc)
    assert acc.failure_diagnostic == first

@pytest.mark.parametrize('name', ['claude','codex','opencode','vibe','antigravity'])
def test_stderr_requires_error_prefix_and_excludes_auth(name):
    backend = get_backend(name)
    assert backend.stderr_usage_limit_diagnostic('quoted usage_limit_reached') is None
    assert backend.stderr_usage_limit_diagnostic('API Error: 401 usage_limit_reached') is None
    assert backend.stderr_usage_limit_diagnostic('API Error: rate_limit_exceeded')['source'] == 'stderr'
    assert backend.usage_limit_diagnostic({'type':'assistant','error':{'code':'usage_limit_reached'}}) is None

@pytest.mark.parametrize('status', ['allowed','allowed_warning','warning'])
def test_claude_warning_is_not_terminal(status):
    assert get_backend('claude').usage_limit_diagnostic({'type':'rate_limit_event','rate_limit_info':{'status':status}}) is None

def test_claude_rejected_reset_and_invalid_reset():
    backend = get_backend('claude')
    for value in [True, float('inf'), None]:
        diagnostic = backend.usage_limit_diagnostic({'type':'rate_limit_event','rate_limit_info':{'status':'rejected','resetsAt':value}})
        assert 'reset_at' not in diagnostic

def test_agy_terminal_error_string():
    assert get_backend('antigravity').usage_limit_diagnostic({'event':'result','result':{'status':'ERROR','error':'insufficient_quota'}})['category'] == 'usage_limit'

@pytest.mark.parametrize('envelope', ['turn.failed', 'error'])
@pytest.mark.parametrize('info', ['usage_limit_reached', {'code': 'usage_limit_reached'}])
def test_codex_typed_error_is_authoritative(envelope, info):
    backend = get_backend('codex')
    event = {'type': envelope, 'error': {'message': 'Limit reached', 'codex_error_info': info}}
    assert backend.usage_limit_diagnostic(event)['category'] == 'usage_limit'
    for status in (401, 403, 503):
        assert backend.usage_limit_diagnostic({**event, 'error': {**event['error'], 'status': status}}) is None
    assert backend.usage_limit_diagnostic({'type': 'item.completed', 'item': {'type': 'error', **event['error']}}) is None

@pytest.mark.parametrize('status', ['running', 'failed', 'cancelled'])
def test_recovered_live_limit_with_lost_owner_requires_attention(tmp_path, monkeypatch, status):
    from polybridge import store
    from test_store import make_record
    diagnostic = {'category': 'usage_limit', 'reason': 'Quota reached', 'source': 'stderr'}
    record = make_record(status=status, failure_diagnostic=diagnostic)
    monkeypatch.setattr(store, 'record_process_alive', lambda record: True)
    monkeypatch.setattr(store.identity, 'identity_check', lambda owner: 'unknown')
    snapshot = store.snapshot(tmp_path, record)
    assert snapshot['status'] == 'running'
    assert snapshot['failure_diagnostic']['settlement'] == 'needs_attention'
    assert store.brief(tmp_path, record)['failure_diagnostic']['settlement'] == 'needs_attention'
    assert any('needs attention' in notice for notice in snapshot['notices'])
    assert 'settlement' not in record.failure_diagnostic


def test_live_and_recovered_briefs_retain_durable_usage_evidence(tmp_path):
    from datetime import datetime, timezone
    from polybridge import store
    from polybridge.tasks import Task
    from test_store import make_record
    diagnostic = {'category': 'usage_limit', 'reason': 'Quota reached', 'source': 'stderr', 'reset_at': 1234}
    record = make_record(status='failed', exit_code=1, failure_diagnostic=diagnostic)
    live = Task(task_id='t', backend='claude', session_id='s', repo_path=tmp_path,
                prompt='work', max_turns=5, log_path=tmp_path / 't.jsonl',
                started_at=datetime.now(timezone.utc))
    live.acc.failure_diagnostic = diagnostic
    assert live.brief()['failure_diagnostic'] == store.brief(tmp_path, record)['failure_diagnostic'] == diagnostic


@pytest.mark.parametrize('name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
@pytest.mark.parametrize('line', ["You've hit your limit", 'Credit balance is too low', 'usage_limit_reached', 'insufficient_quota', 'rate_limit_exceeded', 'rate_limit_error', 'Warning: rate_limit_exceeded'])
def test_plain_quota_stderr_never_authorizes_automatic_fallback(name, line):
    backend = get_backend(name)
    assert backend.stderr_usage_limit_diagnostic(line) is None
    assert backend.workflow_stderr_availability_failure(line) is None


@pytest.mark.parametrize('name,envelope', [('claude', 'error'), ('codex', 'turn.failed'), ('opencode', 'error'), ('vibe', 'error'), ('antigravity', 'result')])
def test_authoritative_quota_remains_separate_from_automatic_availability(name, envelope):
    backend = get_backend(name)
    error = {'code': 'rate_limit_exceeded'}
    event = {'event': 'result', 'result': {'status': 'ERROR', 'error': error}} if name == 'antigravity' else {'type': envelope, 'error': error}
    assert backend.usage_limit_diagnostic(event)['category'] == 'usage_limit'
    assert backend.workflow_availability_failure(event) is None
    line = 'API Error: rate_limit_exceeded'
    assert backend.stderr_usage_limit_diagnostic(line)['category'] == 'usage_limit'
    assert backend.workflow_stderr_availability_failure(line) is None
    assert backend.workflow_stderr_availability_failure('API Error: 503 Service unavailable')


@pytest.mark.parametrize('name', ['claude', 'codex', 'opencode', 'vibe'])
def test_missing_model_still_authorizes_availability_fallback(name):
    backend = get_backend(name)
    assert backend.workflow_stderr_availability_failure('model_not_found')
    assert backend.stderr_usage_limit_diagnostic('API Error: model_not_found') is None


def test_rejected_claude_rate_envelope_does_not_authorize_automatic_fallback():
    backend = get_backend('claude')
    event = {'type': 'rate_limit_event', 'rate_limit_info': {'status': 'rejected'}}
    assert backend.usage_limit_diagnostic(event)
    assert backend.workflow_availability_failure(event) is None


@pytest.mark.parametrize('name,envelope', [('claude', 'error'), ('codex', 'turn.failed'), ('opencode', 'error'), ('vibe', 'error'), ('antigravity', 'result')])
def test_mixed_quota_transport_codes_never_authorize_automatic_fallback(name, envelope):
    backend = get_backend(name)
    error = {'type': 'overloaded_error', 'code': 'rate_limit_exceeded'}
    event = {'event': 'result', 'result': {'status': 'ERROR', 'error': error}} if name == 'antigravity' else {'type': envelope, 'error': error}
    assert backend.workflow_availability_failure(event) is None
    assert backend.usage_limit_diagnostic(event)
    assert backend.workflow_stderr_availability_failure('API Error: rate_limit_exceeded; provider timed out') is None


def test_claude_api_error_with_quota_code_is_not_transport_fallback():
    backend = get_backend('claude')
    event = {'type': 'error', 'error': {'type': 'api_error', 'code': 'rate_limit_error'}}
    assert backend.workflow_availability_failure(event) is None
    assert backend.usage_limit_diagnostic(event)


@pytest.mark.parametrize('name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
@pytest.mark.parametrize('quota', ['API Error: rate_limit_exceeded', 'rate_limit_exceeded', "You've hit your limit", 'Credit balance is too low', 'Warning: rate_limit_exceeded'])
@pytest.mark.parametrize('quota_first', [True, False])
@pytest.mark.parametrize('outage', ['API Error: 503 Service unavailable', 'API Error: provider timed out'])
def test_multiline_quota_diagnostic_never_authorizes_availability(name, quota, quota_first, outage):
    backend = get_backend(name)
    lines = [quota, outage] if quota_first else [outage, quota]
    assert bool(backend.workflow_stderr_availability_failure('\n'.join(lines))) is quota.startswith('Warning:')
    assert backend.workflow_stderr_availability_failure(outage)
    if quota.startswith('Warning:'):
        assert backend.stderr_usage_limit_diagnostic(quota) is None


@pytest.mark.parametrize('name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
@pytest.mark.parametrize('quoted', ['Warning: rate_limit_exceeded', 'Tool quoted rate_limit_exceeded', '"You\'ve hit your limit"'])
def test_quota_warning_or_quoted_prose_does_not_hide_real_outage(name, quoted):
    backend = get_backend(name)
    for lines in ([quoted, 'API Error: 503 Service unavailable'], ['API Error: 503 Service unavailable', quoted]):
        assert backend.workflow_stderr_availability_failure('\n'.join(lines))
        assert backend.stderr_usage_limit_diagnostic(quoted) is None


@pytest.mark.parametrize('name,envelope', [('claude', 'error'), ('codex', 'turn.failed'), ('opencode', 'error'), ('vibe', 'error'), ('antigravity', 'result')])
@pytest.mark.parametrize('quota_first', [True, False])
def test_legacy_stream_quota_cannot_be_hidden_by_another_availability_event(tmp_path, name, envelope, quota_first):
    import json
    from polybridge import workflows as w
    if name == 'antigravity':
        quota = {'event': 'result', 'result': {'status': 'ERROR', 'error': {'code': 'rate_limit_exceeded'}}}
        outage = {'event': 'result', 'result': {'status': 'ERROR', 'error': {'status': 503}}}
    else:
        quota = {'type': envelope, 'error': {'code': 'rate_limit_exceeded'}}
        outage = {'type': envelope, 'error': {'status': 503}}
    path = tmp_path / 'mixed.jsonl'
    events = [quota, outage] if quota_first else [outage, quota]
    path.write_text(''.join(json.dumps(event) + '\n' for event in events))
    assert w.availability_failure({'status': 'failed', 'backend': name, 'raw_stream_log': str(path)}) is None
    # A stderr outage must not bypass the authoritative quota in the stream either.
    assert w.availability_failure({'status': 'failed', 'backend': name, 'raw_stream_log': str(path), 'stderr_tail': ['API Error: 503 Service unavailable']}) is None


@pytest.mark.parametrize('name,envelope', [('claude', 'error'), ('codex', 'turn.failed'), ('opencode', 'error'), ('vibe', 'error'), ('antigravity', 'result')])
@pytest.mark.parametrize('quota', ['rate_limit_exceeded', "You've hit your limit", 'API Error: insufficient_quota'])
@pytest.mark.parametrize('code', ['outage', 'model'])
def test_quota_stderr_veto_applies_to_availability_in_stream(tmp_path, name, envelope, quota, code):
    import json
    from polybridge import workflows as w
    error = {'status': 503} if code == 'outage' else {'code': 'model_not_found'}
    event = {'event': 'result', 'result': {'status': 'ERROR', 'error': error}} if name == 'antigravity' else {'type': envelope, 'error': error}
    path = tmp_path / 'availability.jsonl'
    path.write_text(json.dumps(event) + '\n')
    assert w.availability_failure({'status': 'failed', 'backend': name, 'stderr_tail': [quota], 'raw_stream_log': str(path)}) is None


@pytest.mark.parametrize('name,envelope', [('claude', 'error'), ('codex', 'turn.failed'), ('opencode', 'error'), ('vibe', 'error'), ('antigravity', 'result')])
@pytest.mark.parametrize('quoted', ['Warning: rate_limit_exceeded', 'Tool quoted rate_limit_exceeded', '"You\'ve hit your limit"'])
def test_warning_prose_stderr_does_not_veto_authoritative_stream_outage(tmp_path, name, envelope, quoted):
    import json
    from polybridge import workflows as w
    error = {'status': 503}
    event = {'event': 'result', 'result': {'status': 'ERROR', 'error': error}} if name == 'antigravity' else {'type': envelope, 'error': error}
    path = tmp_path / 'outage.jsonl'
    path.write_text(json.dumps(event) + '\n')
    assert w.availability_failure({'status': 'failed', 'backend': name, 'stderr_tail': [quoted], 'raw_stream_log': str(path)})


@pytest.mark.parametrize('name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
def test_stderr_usage_reason_never_copies_sensitive_provider_line(name):
    from polybridge.backends.workflow_diagnostics import USAGE_LIMIT_REASON
    line = 'API Error: rate_limit_exceeded Authorization: Bearer synthetic-private-token Cookie: synthetic-session api_key=synthetic-key'
    diagnostic = get_backend(name).stderr_usage_limit_diagnostic(line)
    assert diagnostic == {'category': 'usage_limit', 'reason': USAGE_LIMIT_REASON, 'source': 'stderr'}


def test_antigravity_string_usage_reason_is_generic():
    from polybridge.backends.workflow_diagnostics import USAGE_LIMIT_REASON
    error = 'insufficient_quota Authorization: Bearer synthetic-private-token Cookie: synthetic-session'
    diagnostic = get_backend('antigravity').usage_limit_diagnostic({'event': 'result', 'result': {'status': 'ERROR', 'error': error}})
    assert diagnostic == {'category': 'usage_limit', 'reason': USAGE_LIMIT_REASON, 'source': 'stream:result'}


@pytest.mark.parametrize('name', ['claude', 'codex', 'opencode', 'vibe', 'antigravity'])
@pytest.mark.parametrize('source', ['stderr', 'reset'])
async def test_sensitive_stderr_does_not_enter_durable_public_usage_metadata(tmp_path, name, source):
    import json
    from datetime import datetime, timezone
    from polybridge import store, workflows as w
    from polybridge.tasks import Task, TaskRegistry
    from polybridge.workflow_responses import compact
    from test_workflow_delegation import graph
    line = 'API Error: rate_limit_exceeded Authorization: Bearer synthetic-private-token Cookie: synthetic-session api_key=synthetic-key'
    diagnostic = get_backend(name).stderr_usage_limit_diagnostic(line)
    if source == 'reset':
        error = {'code': 'usage_limit_reached', 'reset_at': line}
        envelope = 'turn.failed' if name == 'codex' else 'error'
        event = {'event': 'result', 'result': {'status': 'ERROR', 'error': error}} if name == 'antigravity' else {'type': envelope, 'error': error}
        diagnostic = get_backend(name).usage_limit_diagnostic(event)
    registry = TaskRegistry(log_dir=tmp_path / 'tasks')
    task = Task(task_id='limited', backend=name, session_id='session', repo_path=tmp_path,
                prompt='work', max_turns=None, log_path=tmp_path / 'tasks' / 'limited.jsonl',
                started_at=datetime.now(timezone.utc))
    task.acc.failure_diagnostic = diagnostic
    registry._record_usage_limit(task)
    await task.usage_settlement
    persisted = store.read(registry.log_dir, task.task_id)
    assert persisted.failure_diagnostic == diagnostic
    storage = w.WorkflowStore(tmp_path)
    definition = w.validate_definition(graph())
    run = storage.create_run(definition, 'Work', tmp_path)
    supervisor = w.WorkflowSupervisor(registry, storage)
    supervisor.run_id = run['workflow_run_id']
    activation = supervisor._activation('orchestrator', 'orchestrator')
    supervisor._usage_limit_outcome({'status': 'failed', 'failure_diagnostic': diagnostic}, {'id': 'orchestrator'}, 'orchestrator', activation, definition['orchestrator'], [])
    public = json.dumps({'diagnostic': diagnostic, 'live_brief': task.brief(), 'snapshot': task.snapshot(), 'recovered_brief': store.brief(registry.log_dir, persisted), 'recovery': compact(supervisor.run())})
    for secret in ('synthetic-private-token', 'synthetic-session', 'synthetic-key', 'Authorization', 'Cookie'):
        assert secret not in public


@pytest.mark.parametrize('name,envelope', [('claude', 'error'), ('codex', 'turn.failed'), ('opencode', 'error'), ('vibe', 'error'), ('antigravity', 'result'), ('claude', 'rate_limit_event')])
@pytest.mark.parametrize('value', ['Authorization: Bearer synthetic-private-token', 'Cookie: synthetic-session', 'tomorrow', '', '2026-10-10', '2026-10-10T01:02:03', '2026-02-30T01:02:03Z', '2026-10-10T25:02:03Z', '2026-10-10T01:02:03+01:99', '2026-10-10T01:02:03+24:00', '2026-10-10T01:02:03Z api_key=synthetic-key', True, False, None, float('nan'), float('inf'), float('-inf'), {}, []])
def test_invalid_reset_metadata_is_dropped_without_losing_quota(name, envelope, value):
    if envelope == 'rate_limit_event':
        event = {'type': envelope, 'rate_limit_info': {'status': 'rejected', 'resetsAt': value}}
    else:
        error = {'code': 'usage_limit_reached', 'data': {'reset_at': value}}
        event = {'event': 'result', 'result': {'status': 'ERROR', 'error': error}} if name == 'antigravity' else {'type': envelope, 'error': error}
    diagnostic = get_backend(name).usage_limit_diagnostic(event)
    assert diagnostic['category'] == 'usage_limit'
    assert 'reset_at' not in diagnostic


@pytest.mark.parametrize('value', ['2026-10-10T01:02:03Z', '2026-10-10T01:02:03.123456+05:30', '2026-10-10T01:02:03-07:00', 1791594123, 1791594123.5, 0, -1])
@pytest.mark.parametrize('claude_rate', [False, True])
def test_valid_reset_metadata_is_preserved_unchanged(value, claude_rate):
    event = {'type': 'rate_limit_event', 'rate_limit_info': {'status': 'rejected', 'resetsAt': value}} if claude_rate else {'type': 'error', 'error': {'code': 'usage_limit_reached', 'reset_at': value}}
    assert get_backend('claude').usage_limit_diagnostic(event)['reset_at'] == value
