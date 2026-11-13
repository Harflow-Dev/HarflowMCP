"""Adapter infrastructure checks; a tiny binary is not engine qualification."""
import hashlib
import json
import os
from pathlib import Path
import sys

import pytest

from conformance.oracles import ConformanceError
from conformance.runner import (isolated_environment, main, publish_report, run_binary_suite,
                                validate_operation, validate_report)

BUNDLE = Path(__file__).resolve().parents[2] / 'contracts/v1/manifest.json'
DIGEST = '092299de1f3ed1ac91c55d168ce3f77c47e3d4f6859b9ebefb416393e149aea1'


def fake_engine(tmp_path, addition=''):
    path = tmp_path / 'explicit-test-engine'
    path.write_text('#!' + sys.executable + '\n' + '''import json,os,platform,sys
root=os.environ['HARFLOW_STATE_ROOT']
d=dict(product='harflow',v=1,product_version='0.0.7')
if sys.argv[1:] == ['version-info','--json']:
 d.update(format='harflow.version-info',component='engine',state_root=root,os=sys.platform,architecture=platform.machine(),capabilities=['ctl.v1','state-recovery.v1'],contracts={k:dict(format=f,v=1) for k,f in [('ctl','harflow.ctl'),('setup','harflow.setup'),('events','harflow.event')]})
elif sys.argv[1:] == ['state-inspect','--json']:
 d.update(format='harflow.ctl',result=dict(state_root=root,status='missing',reason='missing_root',cooperative_ownership_only=True))
else: raise SystemExit(99)
''' + addition + '\nprint(json.dumps(d))\n')
    path.chmod(0o700)
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def test_explicit_binary_adapter_smoke_and_identity(tmp_path):
    binary, digest = fake_engine(tmp_path)
    report = run_binary_suite(binary, digest, BUNDLE, DIGEST, tmp_path / 'run')
    assert report['ok'] is True, report
    assert report['target']['sha256'] == digest
    assert report['external_identity_before'] == report['external_identity_after']
    assert len(report['cases']) == 3
    assert not (tmp_path / 'run/smoke/state').exists()
    assert validate_report(tmp_path / 'run/report.json', report['invocation_id']) == report


@pytest.mark.parametrize('bad', ['relative', 'digest', 'symlink'])
def test_binary_selection_rejects_unpinned_or_aliased_target(tmp_path, bad):
    binary, digest = fake_engine(tmp_path)
    if bad == 'relative': binary = Path('relative-engine')
    elif bad == 'digest': digest = '0' * 64
    else:
        link = tmp_path / 'linked-engine'
        link.symlink_to(binary)
        binary = link
    with pytest.raises(ConformanceError):
        run_binary_suite(binary, digest, BUNDLE, DIGEST, tmp_path / 'run')
    assert not (tmp_path / 'run').exists()


def test_binary_drift_cannot_pass(tmp_path):
    binary, digest = fake_engine(tmp_path, "if sys.argv[1] == 'state-inspect':\n open(__file__, 'a').write('# changed')")
    report = run_binary_suite(binary, digest, BUNDLE, DIGEST, tmp_path / 'run')
    assert report['ok'] is False
    assert 'digest mismatch' in report['error']


@pytest.mark.parametrize('operation', [['run','--json'], ['resume','--json'], ['cancel','--json'],
    ['workflow-start','--json'], ['state-reset','--json'], ['version-info','--json','--extra']])
def test_s07_does_not_launch_unsafe_engine_operations(operation):
    with pytest.raises(ConformanceError):
        validate_operation(operation)


def test_environment_is_fresh_and_drops_auth_and_caller_state(tmp_path, monkeypatch):
    monkeypatch.setenv('OPENAI_API_KEY', 'test-value-never-forward')
    monkeypatch.setenv('HARFLOW_TASK_ID', 'old-caller')
    monkeypatch.setenv('PYTHONPATH', '/foreign/editable/source')
    env = isolated_environment(tmp_path)
    assert not {'OPENAI_API_KEY','HARFLOW_TASK_ID','PYTHONPATH'} & env.keys()
    assert env['HOME'] == str(tmp_path / 'home')
    assert env['HARFLOW_STATE_ROOT'] == str(tmp_path / 'state')
    assert not Path(env['HARFLOW_STATE_ROOT']).exists()


def test_report_publication_never_overwrites_file_or_symlink(tmp_path):
    target = tmp_path / 'report.json'
    target.write_text('preserve')
    with pytest.raises(FileExistsError): publish_report(target, {'new': True})
    assert target.read_text() == 'preserve'
    target.unlink()
    outside = tmp_path / 'outside'
    outside.write_text('preserve')
    target.symlink_to(outside)
    with pytest.raises(FileExistsError): publish_report(target, {'new': True})
    assert outside.read_text() == 'preserve'
    assert target.is_symlink()


def test_report_identity_and_symlink_refused(tmp_path):
    target = tmp_path / 'report.json'
    publish_report(target, dict(format='harflow.conformance.report',v=1,invocation_id='known'))
    with pytest.raises(ConformanceError): validate_report(target, 'foreign')
    link = tmp_path / 'link'
    link.symlink_to(target)
    with pytest.raises(ConformanceError): validate_report(link, 'known')


def test_cli_rejects_ambiguous_source_and_binary_selection(tmp_path):
    with pytest.raises(SystemExit) as result:
        main(['--engine-bin', '/explicit', '--engine-sha256', '0'*64, '--source-repo', '/source',
              '--manifest', str(BUNDLE), '--manifest-sha256', DIGEST, '--output', str(tmp_path / 'run')])
    assert result.value.code == 2
    assert not (tmp_path / 'run').exists()


def test_source_snapshot_uses_exact_commit_and_excludes_editable_checkout(tmp_path):
    import subprocess
    from conformance.runner import _snapshot
    repo = Path(__file__).resolve().parents[2]
    commit = '0f10ad30e28e7da1873685cebe70aef687881723'
    destination = tmp_path / 'snapshot'
    identity = _snapshot(repo, commit, destination)
    expected = subprocess.check_output(['/usr/bin/git', '-C', str(repo), 'show', commit + ':src/harflow_mcp/cli.py'])
    assert (destination / 'src/harflow_mcp/cli.py').read_bytes() == expected
    assert identity['commit'] == commit
    assert not (destination / 'conformance/tests/test_runner.py').exists()
    assert not (destination / '.venv').exists()


@pytest.mark.parametrize('commit', ['HEAD', '0f10ad3', '../unsafe', '0' * 39])
def test_source_snapshot_refuses_mutable_or_abbreviated_commit(tmp_path, commit):
    from conformance.runner import _snapshot
    with pytest.raises(ConformanceError):
        _snapshot(Path(__file__).resolve().parents[2], commit, tmp_path / 'snapshot')
    assert not (tmp_path / 'snapshot').exists()


@pytest.fixture
def completed_binary_report(tmp_path):
    binary, digest = fake_engine(tmp_path)
    output = tmp_path / 'run'
    report = run_binary_suite(binary, digest, BUNDLE, DIGEST, output)
    assert report['ok'], report
    return output, report


@pytest.mark.parametrize('tamper', ['empty-cases', 'missing-case', 'exit-code', 'case-ok', 'target-pin',
                                  'missing-spawn', 'spawn-nonce', 'capture-length', 'missing-fakes', 'context-binding'])
def test_success_report_requires_bound_external_evidence(completed_binary_report, tamper):
    output, report = completed_binary_report
    process = report['cases'][0]['process']
    if tamper == 'empty-cases': report['cases'] = []
    elif tamper == 'missing-case': report['cases'].pop()
    elif tamper == 'exit-code': process['returncode'] = 7
    elif tamper == 'case-ok': process['ok'] = False
    elif tamper == 'target-pin': report['target']['sha256'] = '0' * 64
    elif tamper == 'missing-spawn':
        (Path(process['streams']['stdout']['path']).parent / 'spawn.json').unlink()
    elif tamper == 'spawn-nonce':
        spawn_path = Path(process['streams']['stdout']['path']).parent / 'spawn.json'
        spawn = json.loads(spawn_path.read_bytes())
        spawn['nonce'] = 'forged'
        spawn_path.write_text(json.dumps(spawn))
    elif tamper == 'capture-length': process['streams']['stdout']['captured_bytes'] += 1
    elif tamper == 'missing-fakes': report.pop('fake_cases')
    elif tamper == 'context-binding':
        context_path = output / 'context.json'
        context = json.loads(context_path.read_bytes())
        context['invocation_id'] = 'foreign-invocation'
        context_path.write_text(json.dumps(context))
    target = output / 'report.json'
    target.write_text(json.dumps(report))
    with pytest.raises(ConformanceError): validate_report(target, report['invocation_id'])


def test_report_rejects_capture_escape_before_reading_outside(completed_binary_report, monkeypatch):
    import conformance.runner as runner
    output, report = completed_binary_report
    outside = output.parent / 'must-not-read'
    outside.write_bytes(b'private sentinel')
    report['cases'][0]['process']['streams']['stdout']['path'] = str(outside)
    target = output / 'report.json'
    target.write_text(json.dumps(report))
    actual_read = runner.read_regular
    def observed_read(path):
        assert Path(path) != outside, 'validator read escaped capture before containment check'
        return actual_read(path)
    monkeypatch.setattr(runner, 'read_regular', observed_read)
    with pytest.raises(ConformanceError): validate_report(target, report['invocation_id'])


@pytest.mark.parametrize('tamper', ['failed-positive-fake', 'fake-claim', 'external-identity'])
def test_report_success_cannot_override_failed_or_mismatched_evidence(completed_binary_report, tamper):
    output, report = completed_binary_report
    if tamper == 'failed-positive-fake':
        fake = next(row for row in report['fake_cases'] if row['mode'] == 'success')
        fake['process']['ok'] = False
        fake['process']['returncode'] = 7
        ledger = Path(fake['process']['streams']['stdout']['path']).parent / 'ledger.json'
        ledger.write_text(json.dumps(fake['process']))
    elif tamper == 'fake-claim':
        report['fake_cases'][0]['fake_observation']['nonce'] = 'forged-claim'
    else:
        report['external_identity_after'] = {'drift': True}
    assert report['ok'] is True
    target = output / 'report.json'
    target.write_text(json.dumps(report))
    with pytest.raises(ConformanceError): validate_report(target, report['invocation_id'])


def test_report_rejects_dotdot_capture_before_reading_outside(completed_binary_report, monkeypatch):
    import conformance.runner as runner
    output, report = completed_binary_report
    outside = output.parent / 'must-not-read-dotdot'
    outside.write_bytes(b'private sentinel')
    escaped = output / 'smoke' / '..' / '..' / outside.name
    assert escaped.is_relative_to(output)
    assert escaped.resolve() == outside
    report['cases'][0]['process']['streams']['stdout']['path'] = str(escaped)
    target = output / 'report.json'
    target.write_text(json.dumps(report))
    actual_read = runner.read_regular
    def observed_read(path):
        assert Path(os.path.normpath(path)) != outside, 'validator read dotdot escape before containment check'
        return actual_read(path)
    monkeypatch.setattr(runner, 'read_regular', observed_read)
    with pytest.raises(ConformanceError): validate_report(target, report['invocation_id'])


def test_read_only_smoke_refuses_new_broken_state_root_symlink(tmp_path):
    addition = "if sys.argv[1] == 'state-inspect':\n os.symlink('/nonexistent-conformance-target', root)"
    binary, digest = fake_engine(tmp_path, addition)
    output = tmp_path / 'run'
    report = run_binary_suite(binary, digest, BUNDLE, DIGEST, output)
    assert report['ok'] is False
    assert 'created state' in report['error']
    state = output / 'smoke/state'
    assert state.is_symlink()
    assert os.readlink(state) == '/nonexistent-conformance-target'
    assert not state.exists()
