"""Malformed typed targets must refuse without an issuance or a transport crash."""
import pytest

from harflow_mcp import workflow_steering as s
from test_orchestrator_steering_faults import origin, request, fresh


@pytest.mark.parametrize('target', [{}, {'role': 'orchestrator'}, {'workflow_run_id': None}, {'workflow_run_id': 'missing', 'token_id': None}])
async def test_incomplete_target_has_typed_refusal_and_no_issuance(origin, target):
    storage, rid = origin
    payload = request(storage, rid)
    payload['target'] = target
    with pytest.raises(s.SteeringError) as error:
        await s.steer_workflow(rid, payload, root=storage.root)
    assert error.value.code == 'malformed_command'
    assert not fresh(storage, rid)['steering']['turns']
    assert not fresh(storage, rid)['steering']['requests']
