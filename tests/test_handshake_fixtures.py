"""Migration boundary evidence; no installed runtime or provider qualification."""
import hashlib
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
    assert len(names) == len(set(names)) == 55
    assert len(fixture['included']) == 47
    assert set(fixture['included']).isdisjoint(fixture['deferred'])
    assert set(fixture['included']) | set(fixture['deferred']) == set(names)
    # S01/S02 schema identity must survive ctl-only additions.
    canonical = json.dumps(fixture['tools'], sort_keys=True, separators=(',', ':')).encode()
    assert hashlib.sha256(canonical).hexdigest() == '3c0a3e2dfeca9000c0c855e6ee595356f1e664d24f7141b91f07b2c1e8eab12b'


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
