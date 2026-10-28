"""Observable permission protocol failures stop the task and poison its launch receipt."""
import pytest

from harflow_mcp import approval_runtime, approvals, backends
from harflow_mcp.backends import qualification
from test_caller_write_approvals import runtime, tmp_path, runtime_task, event  # noqa: F401


@pytest.mark.asyncio
@pytest.mark.parametrize('decision,delivery', [(None, 'pending'), ('reject', 'sent'), ('approve', 'reserved')])
async def test_success_without_delivered_approval_stops_and_invalidates(runtime, monkeypatch, decision, delivery):
    directory, registry = runtime
    task = runtime_task(directory)
    task.backend = 'claude'
    identity = qualification.Identity('claude', '2.1.310', '/old-binary', 'a' * 64, 'darwin')
    task.qualification_identity = identity
    invalidations = []
    def invalidate(backend, feature, reason, **kw):
        assert registry.stopped == 1, 'transport must stop before waiting on cache publication'
        invalidations.append((feature, kw['identity'], reason))
    monkeypatch.setattr(qualification, 'invalidate', invalidate)
    await approval_runtime.observe(registry, task, backends.get('claude'), event('Bash', {'command': 'must wait'}))
    row, = approvals.rows(directory, task.task_id)
    monkeypatch.setattr(approvals, 'rows', lambda *_a: [{**row, 'effective_decision': decision, 'delivery': delivery}])
    monkeypatch.setattr(approvals, 'invalidate', lambda *_a: None)
    frame = {'type': 'user', 'message': {'content': [{'type': 'tool_result', 'tool_use_id': row['tool_use_id'], 'content': 'executed'}]}}
    await approval_runtime.observe(registry, task, backends.get('claude'), frame)
    assert registry.stopped == 1
    assert all(captured == identity for _feature, captured, _reason in invalidations)
    assert {feature for feature, _identity, _reason in invalidations} == {'caller_approval_tools', 'caller_write_tools'}
    assert any('without a delivered approval' in reason for _, _, reason in invalidations)
