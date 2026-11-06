"""Self-checks for the C1 candidates: exact bytes, independent expectations and non-advertisement."""
import json
from pathlib import Path
import shutil
import sys

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / 'scripts'))

from c1_candidates import common, permission, result, survivors  # noqa: E402

CANDIDATES = [result.CANDIDATE, survivors.CANDIDATE, permission.CANDIDATE]
IDS = [c['name'] for c in CANDIDATES]
CAPABILITIES = {c['capability'] for c in CANDIDATES}


def load(candidate, name):
    return json.loads((REPO / candidate['directory'] / name).read_text())


@pytest.mark.parametrize('candidate', CANDIDATES, ids=IDS)
def test_committed_bytes_are_exactly_the_generated_bytes(candidate):
    common.check(candidate, REPO)


@pytest.mark.parametrize('candidate', CANDIDATES, ids=IDS)
def test_manifest_identity_and_specifications(candidate):
    directory = REPO / candidate['directory']
    manifest_raw = (directory / 'manifest.json').read_bytes()
    manifest = json.loads(manifest_raw)
    assert (directory / 'manifest.sha256').read_text() == common.digest(manifest_raw) + '\n'
    assert manifest['format'] == common.COMPONENT and manifest['v'] == 1
    assert (manifest['status'], manifest['provenance'], manifest['python_runtime_support'], manifest['advertised']) == \
        ('specified_unimplemented', 'synthetic_specification', False, False)
    assert manifest['candidate'] == candidate['name'] and manifest['capability'] == candidate['capability']
    for spec in manifest['specifications']:
        assert common.digest((REPO / spec['path']).read_bytes()) == spec['sha256']
    for entry in manifest['files']:
        raw = (directory / entry['filename']).read_bytes()
        assert (len(raw), common.digest(raw)) == (entry['bytes'], entry['sha256'])
        assert common.classify(candidate, raw) == entry['expected']


@pytest.mark.parametrize('candidate', CANDIDATES, ids=IDS)
def test_each_candidate_has_accepting_and_rejecting_specimens(candidate):
    expected = [entry['expected'] for entry in json.loads((REPO / candidate['directory'] / 'manifest.json').read_text())['files']]
    assert any(e['parse'] == 'valid' for e in expected)
    assert {'invalid', 'unsupported'} <= {e['parse'] for e in expected}
    assert any(e.get('reason') == 'envelope' for e in expected)


def test_capabilities_are_reserved_and_not_advertised():
    from harflow_mcp import wire_contracts
    assert not CAPABILITIES & set(wire_contracts.CAPABILITIES)
    v1 = (REPO / 'contracts/v1').glob('*.json')
    text = ''.join(p.read_text() for p in v1)
    for name in CAPABILITIES | {c['name'] for c in CANDIDATES}:
        assert name not in text
    source = ''.join(p.read_text() for p in (REPO / 'src').rglob('*.py'))
    for name in CAPABILITIES:
        assert name not in source


def test_expected_results_are_hand_authored_not_taken_from_the_evaluator(monkeypatch):
    """A wrong expectation must stop generation: the evaluator is checked, never the source of truth."""
    original = result.cases

    def wrong():
        cases = original()
        value, expected = cases['settled.json']
        cases['settled.json'] = (value, {**expected, 'workflow': 'failed'})
        return cases
    candidate = {**result.CANDIDATE, 'cases': wrong}
    with pytest.raises(ValueError, match='independent oracle disagreement'):
        common.expected_assets(candidate, REPO)


def test_tamper_extra_and_missing_files_are_refused(tmp_path):
    out = tmp_path / 'out'
    common.generate(result.CANDIDATE, REPO, out)
    common.check(result.CANDIDATE, REPO, out)
    target = out / 'settled.json'
    original = target.read_bytes()
    target.write_bytes(original.replace(b'"succeeded"', b'"failed"', 1))
    with pytest.raises(ValueError, match='bytes differ'):
        common.check(result.CANDIDATE, REPO, out)
    target.write_bytes(original)
    (out / 'extra.json').write_text('{}')
    with pytest.raises(ValueError, match='inventory'):
        common.check(result.CANDIDATE, REPO, out)
    (out / 'extra.json').unlink()
    target.unlink()
    with pytest.raises(ValueError, match='inventory'):
        common.check(result.CANDIDATE, REPO, out)


def test_the_110_specimen_pins_the_literal_decision():
    specimen = load(result.CANDIDATE, 'incidental-failed-keeps-success.json')
    assert specimen['input']['requirement'] == 'incidental' and specimen['input']['obligations'] == 'failed'
    assert specimen['document'] == {'agent_result': 'succeeded',
                                    'background': {'requirement': 'incidental', 'obligations': 'failed', 'inventory': 'complete'},
                                    'settlement': {'workflow': 'succeeded', 'reason': 'succeeded', 'release': 'evidence_complete'}}
    unknown = load(result.CANDIDATE, 'unknown-requirement-never-incidental.json')
    assert unknown['document']['settlement']['workflow'] == 'blocked'


def test_118_specimens_never_release_early_or_imply_settlement():
    early = common.classify(survivors.CANDIDATE, (REPO / survivors.CANDIDATE['directory'] / 'retention-early-release-is-refused.json').read_bytes())
    assert early == {'parse': 'invalid', 'reason': 'projection_mismatch'}
    held = load(survivors.CANDIDATE, 'retention-survivors-hold-slot.json')
    assert held['document']['slot']['state'] == 'retained' and held['input']['survivors'] == 'present'
    released = load(survivors.CANDIDATE, 'retention-human-release.json')['document']['slot']
    assert (released['settlement'], released['replay']) == ('not_implied', 'not_authorized')


def test_d6_specimens_never_overclaim_os_enforcement():
    for name in ('report-stock-claude-tool-level.json', 'report-explicit-tool-level.json', 'report-unknown-establishment.json'):
        assert load(permission.CANDIDATE, name)['document']['enforcement']['os_enforced'] is False
    assert common.classify(permission.CANDIDATE, (REPO / permission.CANDIDATE['directory'] / 'report-tool-level-claims-os.json').read_bytes()) == \
        {'parse': 'invalid', 'reason': 'overclaim'}


@pytest.mark.parametrize('raw, parse, reason', [
    (b'{"format":"harflow.c1.candidate","format":"x"}', 'invalid', 'encoding'),
    (b'{"x":NaN}', 'invalid', 'encoding'),
    (b'\xff', 'invalid', 'encoding'),
    (b'x' * 24577, 'invalid', 'budget'),
    (b'[]', 'invalid', 'envelope'),
    (b'BOOLEAN_VERSION', 'invalid', 'envelope'),
])
@pytest.mark.parametrize('candidate', CANDIDATES, ids=IDS)
def test_envelope_and_encoding_rules(candidate, raw, parse, reason):
    if raw == b'BOOLEAN_VERSION':
        raw = json.dumps({'format': 'harflow.c1.candidate', 'v': True, 'candidate': candidate['name'],
                          'kind': candidate['kinds'][0], 'document': {}}).encode()
    assert common.classify(candidate, raw) == {'parse': parse, 'reason': reason}


def test_generator_script_check_mode_passes():
    import subprocess
    done = subprocess.run([sys.executable, str(REPO / 'scripts/generate-c1-candidate-fixtures.py'), '--check'], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
