"""Canonical task-artifacts specimens conform to the public ctl producer."""
import importlib.util
import json
from pathlib import Path

from harflow_mcp.wire_contracts import CAPABILITIES

ROOT = Path(__file__).resolve().parents[1]
BUNDLE = ROOT / 'contracts/v1'


def generator():
    spec = importlib.util.spec_from_file_location('artifact_fixtures', ROOT / 'scripts/generate-artifact-fixtures.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_artifact_fixtures_match_fresh_ctl_dispatches():
    module = generator()
    first = module.generate()
    assert first == module.generate()
    for path, expected in first.items():
        assert path.read_bytes() == expected, path


def test_artifact_fixture_semantics_and_capability():
    assert CAPABILITIES[-1] == 'task-artifacts.v1'
    manifest = json.loads((BUNDLE / 'manifest.json').read_bytes())
    def document(label):
        name = f'ctl-task-artifacts-{label}.json'
        case = manifest['cases'][name]
        assert case['required_capabilities'] == ['task-artifacts.v1']
        assert case['provenance']
        doc = json.loads((BUNDLE / name).read_bytes())
        assert case['exit_code'] == (1 if 'error' in doc else 0)
        return doc
    first, second = document('populated')['result'], document('next-page')['result']
    assert first['has_more'] and first['next_cursor']
    assert second['has_more'] is False and second['next_cursor'] is None
    rows = first['items'] + second['items']
    assert [row['availability']['state'] for row in rows] == ['available', 'changed', 'deleted', 'unavailable']
    assert {row['provenance']['method'] for row in rows} == {'verified_process_lineage', 'observed_backend_plan', 'engine_output'}
    assert len({row['artifact_id'] for row in rows}) == first['count'] == second['count'] == 4
    assert all(row['note'].startswith(' Complete note ') and row['note'].endswith('\n Preserve whitespace. ') for row in rows)
    empty, unavailable = document('empty')['result'], document('unavailable')['result']
    assert empty == unavailable | {'state': 'available'}
    assert empty['count'] == 0 and empty['items'] == []
    assert unavailable['state'] == 'unavailable'  # Its count is a placeholder, never authoritative.
    assert 'data' not in document('error')['error']
    assert document('page-error')['error']['data'] == {'reason': 'cursor_invalid'}
    assert document('preparing')['error']['data'] == {'reason': 'ownership_preparing'}
    negative = manifest['cases']['invalid-s05-missing-task-artifacts.json']
    assert negative['required_capabilities'] == ['task-artifacts.v1'] and negative['valid'] is False
