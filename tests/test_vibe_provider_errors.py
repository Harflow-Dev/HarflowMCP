"""Payment errors measured with an isolated, loopback-only Vibe provider."""
import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from harflow_mcp.backends.base import Accumulator
from harflow_mcp.backends.vibe import VibeBackend
from harflow_mcp.backends.vibe_provider_errors import DETAIL_PREFIX, PAYMENT_REASON
from harflow_mcp.backends.workflow_diagnostics import USAGE_LIMIT_REASON

FIXTURE = json.loads((Path(__file__).parent / 'fixtures/provider-errors/vibe-3.0.1-http402.json').read_text())


def test_measured_stderr_402_keeps_usage_cause_without_final_prose():
    acc = Accumulator()
    for line in FIXTURE['stderr']:
        VibeBackend.observe_stderr(acc, line)
    assert acc.failure_diagnostic == {'category': 'usage_limit', 'reason': USAGE_LIMIT_REASON,
                                      'source': 'stderr:vibe_http_error', 'http_status': 402}
    assert acc.error_result_seen and not acc.saw_final_message
    assert VibeBackend().classify(acc, FIXTURE['exit_code']) == 'failed'
    assert VibeBackend().continuation_reason(acc, FIXTURE['exit_code']) is None
    assert 'reset_at' not in acc.failure_diagnostic


@pytest.mark.parametrize('detail', ['', DETAIL_PREFIX + 'Payment declined', DETAIL_PREFIX + 'Usage limit exceeded Authorization: Bearer synthetic-secret', 'Usage limit exceeded'])
def test_unrecognized_payment_is_actionable_but_never_quota(detail):
    acc = Accumulator()
    VibeBackend.observe_stderr(acc, FIXTURE['stderr'][1])
    if detail:
        VibeBackend.observe_stderr(acc, detail)
    assert acc.failure_diagnostic['category'] == 'provider_error'
    assert acc.failure_diagnostic['reason'] == PAYMENT_REASON
    assert 'synthetic-secret' not in json.dumps(acc.failure_diagnostic)


def test_detail_requires_adjacent_error_and_is_per_task():
    first, second = Accumulator(), Accumulator()
    VibeBackend.observe_stderr(first, FIXTURE['stderr'][1])
    VibeBackend.observe_stderr(second, FIXTURE['stderr'][2])
    VibeBackend.observe_stderr(first, 'unrelated line')
    VibeBackend.observe_stderr(first, FIXTURE['stderr'][2])
    assert second.failure_diagnostic is None
    assert first.failure_diagnostic['category'] == 'provider_error'
    for line in (FIXTURE['stderr'][1], FIXTURE['stderr'][2]):
        VibeBackend.observe_stderr(first, line)
    assert first.failure_diagnostic['category'] == 'usage_limit'


@pytest.mark.parametrize('code,category', [('usage_limit', 'usage_limit'), ('insufficient_quota', 'usage_limit'), ('payment_required', 'provider_error'), ('authentication_error', 'provider_error'), ({'malformed': True}, 'provider_error')])
def test_structured_402_requires_known_provider_code(code, category):
    acc = Accumulator()
    VibeBackend().ingest({'type': 'error', 'error': {'status': 402, 'type': code, 'message': 'Authorization: Bearer synthetic-secret', 'reset_at': 'tomorrow'}}, acc)
    assert acc.failure_diagnostic['category'] == category
    assert acc.failure_diagnostic['http_status'] == 402
    if category == 'usage_limit':
        assert acc.failure_diagnostic['provider_code'] == code
    assert 'reset_at' not in acc.failure_diagnostic
    assert 'synthetic-secret' not in json.dumps(acc.failure_diagnostic)


@pytest.mark.parametrize('status', [401, 403, 500, 503, True])
def test_other_status_does_not_establish_vibe_payment_quota(status):
    assert VibeBackend.usage_limit_diagnostic({'type': 'error', 'error': {'status': status, 'type': 'usage_limit'}}) is None


async def test_drain_persists_upgraded_cause_and_notifies_once(tmp_path):
    from harflow_mcp.tasks import Task, _drain_stderr
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.state_ownership import state_session
    layout = resolve_layout(root=tmp_path)
    task = Task(task_id='limited', backend='vibe', session_id='session', repo_path=tmp_path,
                prompt='work', max_turns=None, log_path=tmp_path / 'limited.jsonl',
                started_at=datetime.now(timezone.utc))
    class Registry:
        state_layout = layout
        def __init__(self): self.persisted, self.limits = [], []
        def persist(self, task): self.persisted.append(task.acc.failure_diagnostic.copy())
        def _record_usage_limit(self, task): self.limits.append(task.acc.failure_diagnostic.copy())
    registry = Registry()
    reader = asyncio.StreamReader()
    reader.feed_data(('\n'.join(FIXTURE['stderr']) + '\n').encode())
    reader.feed_eof()
    task.proc = SimpleNamespace(stderr=reader)
    with state_session(layout):
        await _drain_stderr(task, registry)
    assert registry.persisted == [{'category': 'provider_error', 'reason': PAYMENT_REASON, 'source': 'stderr:vibe_http_error', 'http_status': 402}]
    assert registry.limits == [task.acc.failure_diagnostic]
    assert task.acc.failure_diagnostic['category'] == 'usage_limit'


def test_workflow_retains_generic_diagnostic_without_promoting_it_to_quota():
    from harflow_mcp import workflows
    from harflow_mcp.workflow_delegation import normalize_result
    snapshot = {'execution_failure': 'No final prose', 'status': 'failed', 'failure_diagnostic': {'category': 'provider_error', 'reason': PAYMENT_REASON, 'source': 'stderr:vibe_http_error', 'http_status': 402}}
    assert workflows.failure_diagnostic(snapshot, 'work') == PAYMENT_REASON
    assert workflows.usage_limit(snapshot) is None
    assert normalize_result({}, snapshot, [])['result']['failure_diagnostic'] == snapshot['failure_diagnostic']
    assert workflows.availability_failure({**snapshot, 'backend': 'vibe', 'stderr_tail': ['API Error: 503 Service unavailable']}) is None
    assert workflows.availability_failure({'status': 'failed', 'backend': 'vibe', 'stderr_tail': [*FIXTURE['stderr'], 'API Error: 503 Service unavailable']}) is None


def test_safe_diagnostic_survives_persistence_status_and_terminal_notification(tmp_path):
    from harflow_mcp import notifications, store
    from harflow_mcp.state_layout import resolve_layout
    from harflow_mcp.state_ownership import state_session
    from test_store import make_record
    acc = Accumulator()
    VibeBackend().ingest({'type': 'error', 'error': {'status': 402, 'type': 'usage_limit', 'message': 'api_key=synthetic-secret'}}, acc)
    layout = resolve_layout(root=tmp_path / 'state')
    with state_session(layout):
        layout.tasks_directory.mkdir()
        record = make_record(status='failed', exit_code=2, failure_diagnostic=acc.failure_diagnostic)
        store.write(layout.tasks_directory, record)
        persisted = store.read(layout.tasks_directory, record.task_id)
        assert persisted.failure_diagnostic == acc.failure_diagnostic
        assert store.snapshot(layout.tasks_directory, persisted)['failure_diagnostic'] == acc.failure_diagnostic
        payload = json.loads(notifications._payload({'id': 'notification'}, persisted))
        assert payload['failure_diagnostic'] == acc.failure_diagnostic
        assert 'synthetic-secret' not in json.dumps(payload)
