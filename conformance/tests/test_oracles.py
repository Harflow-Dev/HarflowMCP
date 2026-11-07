"""Independent contract expectations; never import engine validators or test helpers."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from conformance.oracles import (
    ConformanceError, strict_json, validate_envelope, validate_version_info,
    verify_manifest,
)

BUNDLE = Path(__file__).resolve().parents[2] / 'contracts' / 'v1'
DIGEST = '1a08593b8491c440503c616521b85e8a3d21f0051a93bc13948c4b0371de2e9d'


def envelope(format='harflow.ctl'):
    return dict(product='harflow', format=format, v=1, product_version='0.0.7')


def handshake():
    return dict(**envelope('harflow.version-info'), component='engine',
                contracts={name: {'format': fmt, 'v': 1} for name, fmt in
                           [('ctl', 'harflow.ctl'), ('setup', 'harflow.setup'), ('events', 'harflow.event')]},
                capabilities=['ctl.v1', 'version-info.v1'], os='darwin', architecture='arm64',
                state_root='/tmp/isolated-state')


@pytest.mark.parametrize('raw', [b'{', b'{} {}', b'{"x":NaN}', b'{"x":Infinity}',
                                      b'{"x":1,"x":2}', b'"\xff"'])
def test_malformed_or_ambiguous_json_is_refused(raw):
    with pytest.raises(ConformanceError):
        strict_json(raw)


def test_unknown_fields_and_patch_versions_are_additive():
    document = envelope()
    document.update(product_version='0.0.999+build.1', future={'nested': [1, True, None]})
    validate_envelope(document, 'harflow.ctl')


@pytest.mark.parametrize('change', [
    {'v': True}, {'v': 2}, {'product': 'foreign'}, {'format': 'harflow.setup'},
    {'product_version': '0.00.7'}, {'product_version': '0.0.7-alpha'},
    {'product_version': '0.1.0'}, {'product_version': '0.0.2147483648'},
])
def test_invalid_live_envelopes_are_refused(change):
    document = envelope()
    document.update(change)
    with pytest.raises(ConformanceError):
        validate_envelope(document, 'harflow.ctl')


def test_historical_event_version_is_provenance():
    document = envelope('harflow.event')
    document.update(product_version='1.1.0', kind='future-event', future=True)
    validate_envelope(document, 'harflow.event')


def test_handshake_requires_root_platform_and_capability():
    document = handshake()
    document['capabilities'].append('future.v9')
    document['future'] = {'ignored': True}
    validate_version_info(document, '/tmp/isolated-state', required_capabilities=('ctl.v1',),
                          expected_os='darwin', expected_arch='arm64')
    for kwargs in [dict(root='/tmp/other'), dict(root='/tmp/isolated-state', required_capabilities=('missing.v1',)),
                   dict(root='/tmp/isolated-state', expected_os='linux'),
                   dict(root='/tmp/isolated-state', expected_arch='x86_64')]:
        with pytest.raises(ConformanceError):
            validate_version_info(document, **kwargs)


@pytest.mark.parametrize('field', ['product', 'format', 'v', 'product_version'])
def test_missing_envelope_identity_is_refused(field):
    document = envelope()
    del document[field]
    with pytest.raises(ConformanceError):
        validate_envelope(document, 'harflow.ctl')


def test_manifest_matches_reviewed_fixture_identity():
    manifest = BUNDLE / 'manifest.json'
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == DIGEST
    document = verify_manifest(manifest, expected_sha256=DIGEST)
    assert len(document['files']) == 295


def copied_bundle(tmp_path):
    target = tmp_path / 'bundle'
    shutil.copytree(BUNDLE, target)
    return target


def test_fixture_tamper_is_refused(tmp_path):
    target = copied_bundle(tmp_path)
    (target / 'ctl-success.json').write_bytes(b'{}')
    with pytest.raises(ConformanceError):
        verify_manifest(target / 'manifest.json', expected_sha256=DIGEST)


def test_manifest_tamper_is_refused(tmp_path):
    target = copied_bundle(tmp_path)
    manifest = target / 'manifest.json'
    manifest.write_bytes(manifest.read_bytes() + b' ')
    with pytest.raises(ConformanceError):
        verify_manifest(manifest, expected_sha256=DIGEST)


@pytest.mark.parametrize('malice', ['traversal', 'symlink'])
def test_manifest_cannot_escape_bundle_even_with_matching_manifest_hash(tmp_path, malice):
    target = copied_bundle(tmp_path)
    manifest = target / 'manifest.json'
    document = json.loads(manifest.read_bytes())
    if malice == 'traversal':
        document['files']['../outside.json'] = hashlib.sha256(b'{}').hexdigest()
        (tmp_path / 'outside.json').write_bytes(b'{}')
    else:
        fixture = target / 'ctl-success.json'
        outside = tmp_path / 'outside.json'
        outside.write_bytes(fixture.read_bytes())
        fixture.unlink()
        fixture.symlink_to(outside)
    manifest.write_text(json.dumps(document))
    with pytest.raises(ConformanceError):
        verify_manifest(manifest, expected_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest())


@pytest.mark.parametrize('change', [
    {'contracts': []}, {'contracts': None}, {'contracts': {'ctl': None}},
    {'capabilities': None}, {'capabilities': 'ctl.v1'},
    {'component': 'other'}, {'state_root': None},
])
def test_malformed_handshake_has_typed_refusal(change):
    document = handshake()
    document.update(change)
    with pytest.raises(ConformanceError):
        validate_version_info(document, '/tmp/isolated-state', required_capabilities=('ctl.v1',))


def test_event_page_checks_each_writer_but_preserves_additive_payload():
    from conformance.oracles import validate_event_page
    event = dict(**envelope('harflow.event'), kind='new-kind', metadata={'v': False, 'format': 'opaque'})
    event['product_version'] = '1.1.0'
    page = dict(**envelope(), result={'events': [event]}, future={'product': 'opaque'})
    assert validate_event_page(page) == page
    for change in [{'v': True}, {'v': 2}, {'product': 'foreign'}, {'format': 'harflow.ctl'},
                   {'product_version': 'broken'}]:
        invalid = copy.deepcopy(page)
        invalid['result']['events'][0].update(change)
        with pytest.raises(ConformanceError): validate_event_page(invalid)


@pytest.mark.parametrize('change', [None, {'pid': True}, {'pid': 1235}, {'nonce': 'forged'}, {'argv': ['forged']}])
def test_fake_cannot_certify_or_forge_parent_observation(change):
    from conformance.oracles import validate_fake_observation
    ledger = dict(pid=1234, nonce='parent-nonce', argv=['python','fake.py','success'])
    observation = dict(pid=1234, nonce='parent-nonce', argv=['success'])
    if change is None:
        observation = None
    else:
        observation.update(change)
    with pytest.raises(ConformanceError): validate_fake_observation(observation, ledger)


def test_matching_fake_is_only_corroboration():
    from conformance.oracles import validate_fake_observation
    ledger = dict(pid=1234, nonce='parent-nonce', argv=['python','fake.py','success'])
    observation = dict(pid=1234, nonce='parent-nonce', argv=['success'])
    assert validate_fake_observation(observation, ledger) == observation


def test_nonregular_fifo_is_refused_without_waiting_for_writer(tmp_path):
    import os
    import subprocess
    import sys
    fifo = tmp_path / 'fifo'
    os.mkfifo(fifo)
    program = '''import sys
from conformance.oracles import ConformanceError, read_regular
try:
    read_regular(sys.argv[1])
except ConformanceError:
    raise SystemExit(0)
raise SystemExit(3)
'''
    result = subprocess.run([sys.executable, '-c', program, str(fifo)],
                            cwd=Path(__file__).resolve().parents[2],
                            stdin=subprocess.DEVNULL, capture_output=True, timeout=2)
    assert result.returncode == 0, result.stderr
