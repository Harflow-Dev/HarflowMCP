"""Migration boundary evidence; no installed runtime or provider qualification."""
import hashlib
from copy import deepcopy
import importlib.util
import json
from pathlib import Path

from harflow_mcp.wire_contracts import CAPABILITIES

ROOT = Path(__file__).parents[1]


def generator():
    spec = importlib.util.spec_from_file_location('handshake_fixtures', ROOT / 'scripts/generate-handshake-fixtures.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_launch_and_schema_fixtures_match_current_public_serializers():
    for path, expected in generator().generate().items():
        assert path.read_bytes() == expected, path


def test_included_mcp_schema_scope_preserves_python_surface():
    fixture = json.loads((ROOT / 'contracts/mcp-v1/tools.json').read_text())
    names = [tool['name'] for tool in fixture['tools']]
    assert len(names) == len(set(names)) == 63
    assert len(fixture['included']) == 55
    assert set(fixture['included']).isdisjoint(fixture['deferred'])
    assert set(fixture['included']) | set(fixture['deferred']) == set(names)
    additions = {'get_workflow_steering', 'get_workflow_steering_history', 'steer_workflow',
        'replan_workflow_steering', 'prepare_task_conversation', 'send_conversation_message',
        'control_conversation_message', 'commit_conversation_result'}
    assert additions <= set(fixture['included'])
    legacy = [tool for tool in fixture['tools'] if tool['name'] not in additions]
    assert len(legacy) == 55
    # Only start/resume add an optional profile selector. Preserve all 53 other
    # legacy schemas, including descriptions, and all 55 original input shapes.
    unchanged = [tool for tool in legacy if tool['name'] not in {'start_task', 'resume_task'}]
    assert len(unchanged) == 53
    canonical = json.dumps(unchanged, sort_keys=True, separators=(',', ':')).encode()
    assert hashlib.sha256(canonical).hexdigest() == '9ab63df7b090de8e33a1c1bbd80080262f9090227e4f212ac73d3da373f2cb85'
    normalized = deepcopy(legacy)
    for tool in normalized:
        if tool['name'] in {'start_task', 'resume_task'}:
            profile = tool['inputSchema']['properties'].pop('conversation_profile')
            assert profile == {'anyOf': [{'type': 'string'}, {'type': 'null'}],
                               'default': None, 'title': 'Conversation Profile'}
            assert 'conversation_profile' not in tool['inputSchema'].get('required', [])
    shapes = [{'name': tool['name'], 'inputSchema': tool['inputSchema']} for tool in normalized]
    canonical = json.dumps(shapes, sort_keys=True, separators=(',', ':')).encode()
    assert hashlib.sha256(canonical).hexdigest() == 'a4f6d7d5ce8d6ad9c486697fafef67962a26d35bb24b1c49394e933cd7552282'


def test_s04_capabilities_remain_advertised_with_canonical_examples():
    version = json.loads((ROOT / 'contracts/v1/version-info.json').read_text())
    manifest = json.loads((ROOT / 'contracts/v1/manifest.json').read_text())
    required = {
        'task-event-page.v1': 'ctl-task-events-newest.json',
        'task-prompt.v1': 'ctl-task-prompt-available.json',
        'task-successors.v1': 'ctl-task-page-successors-found.json',
        'workflow-live-activity.v1': 'ctl-workflow-inspect-live-running-attempt.json',
        'statistics-cost-model.v1': 'ctl-statistics-page-cost-reported-and-silent.json',
        'task-changes.v1': 'ctl-task-changes-available-listing.json',
    }
    assert version['capabilities'] == list(CAPABILITIES)
    for capability, name in required.items():
        assert capability in version['capabilities']
        assert capability in manifest['cases'][name]['required_capabilities']
