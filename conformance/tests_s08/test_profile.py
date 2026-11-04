"""Profile fail-closed unit probes; no engine operation is executed here."""
import json
from pathlib import Path

import pytest

from conformance.oracles import ConformanceError
from conformance.s08 import profile as module


def stub_profile(tmp_path, tasks=False):
    value = module.SourceProfile(tmp_path, 'a' * 40, tmp_path / 'output', tasks_enabled=tasks)
    value.state_root = tmp_path / 'state'
    value.workspace = tmp_path
    value.env = {}
    value.runtime_identity = {'platform':'darwin','architecture':'arm64'}
    value.shim = tmp_path / 'shim'
    value.initial_fingerprint = {'fixed': 'identity'}
    value.fingerprint = lambda: dict(value.initial_fingerprint)
    return value


def handshake(root):
    return dict(product='harflow', format='harflow.version-info', v=1,
                product_version='0.0.7', component='engine',
                contracts={key: dict(format=value, v=1) for key, value in
                           [('ctl', 'harflow.ctl'), ('setup', 'harflow.setup'), ('events', 'harflow.event')]},
                capabilities=['ctl.v1', 'version-info.v1'], os='darwin', architecture='arm64',
                state_root=str(root))


@pytest.mark.parametrize('argv', [[], ['run'], ['run','--json'], ['install','--json'],
                                  ['workflow-run','--json'], ['qualify','--json'], ['task','--json']])
def test_task_gate_and_unsafe_commands_refuse_before_invocation(tmp_path, argv):
    value = stub_profile(tmp_path)
    value._invoke = lambda argv: pytest.fail('refusal must precede any subprocess')
    with pytest.raises(ConformanceError):
        value.command(argv)
    assert value.records == []


def test_every_allowed_operation_gets_fresh_validated_handshake(tmp_path):
    value = stub_profile(tmp_path)
    calls = []
    def invoke(argv):
        calls.append(argv)
        return {'document': handshake(value.state_root)}
    value._invoke = invoke
    value.command(['state-inspect','--json'])
    value.command(['state-inspect','--json'])
    assert calls == [['version-info','--json'], ['state-inspect','--json'],
                     ['version-info','--json'], ['state-inspect','--json']]


def test_handshake_wrong_root_or_missing_capability_blocks_operation(tmp_path):
    value = stub_profile(tmp_path)
    calls = []
    def invoke(argv):
        calls.append(argv)
        return {'document': handshake(tmp_path / 'foreign')}
    value._invoke = invoke
    with pytest.raises(ConformanceError):
        value.command(['state-inspect','--json'])
    assert calls == [['version-info','--json']]
    calls.clear()
    value._invoke = lambda argv: {'document': handshake(value.state_root)}
    with pytest.raises(ConformanceError):
        value.command(['state-inspect','--json'], requiredcap=('not-supported.v1',))


@pytest.mark.parametrize('fault', ['timed_out','capture_exceeded','pipes_drained','missing_returncode','unknown_cleanup','missing_cleanup'])
def test_unknown_command_outcome_is_recorded_once_and_never_replayed(tmp_path, monkeypatch, fault):
    value = stub_profile(tmp_path)
    calls = []
    def run(*args, **kwargs):
        calls.append(args)
        record = dict(timed_out=False, capture_exceeded=False, pipes_drained=True,
                      returncode=0, cleanup='group_quiescent_os_observed')
        if fault == 'missing_returncode':
            record['returncode'] = None
        elif fault == 'unknown_cleanup':
            record['cleanup'] = 'unknown'
        elif fault == 'missing_cleanup':
            record.pop('cleanup')
        else:
            record[fault] = fault != 'pipes_drained'
        return record
    monkeypatch.setattr(module, 'run_process', run)
    with pytest.raises(ConformanceError, match='never replay'):
        value._invoke(['state-inspect','--json'])
    assert len(calls) == len(value.records) == 1


def test_source_or_overlay_fingerprint_drift_blocks_spawn(tmp_path, monkeypatch):
    value = stub_profile(tmp_path)
    value.fingerprint = lambda: {'fixed':'changed'}
    monkeypatch.setattr(module, 'run_process', lambda *args, **kwargs: pytest.fail('drifted profile spawned'))
    with pytest.raises(ConformanceError, match='fingerprint differs'):
        value._invoke(['version-info','--json'])
    assert value.records == []


def test_mid_operation_fingerprint_drift_is_not_replayed(tmp_path, monkeypatch):
    value = stub_profile(tmp_path)
    fingerprints = iter([value.initial_fingerprint, {'fixed':'changed'}])
    value.fingerprint = lambda: next(fingerprints)
    calls = []
    monkeypatch.setattr(module, 'run_process', lambda *args, **kwargs: calls.append(args) or {})
    with pytest.raises(ConformanceError, match='changed during operation'):
        value._invoke(['version-info','--json'])
    assert len(calls) == len(value.records) == 1


def test_environment_identity_is_part_of_active_profile_fingerprint(tmp_path):
    value = stub_profile(tmp_path)
    value.source = tmp_path / 'source'
    value.source.mkdir()
    value.output.mkdir()
    (value.output / 'overlay').mkdir()
    binary = tmp_path / 'python-identity'
    binary.write_bytes(b'fixed-interpreter-identity')
    purelib = tmp_path / 'site-packages'
    purelib.mkdir()
    value.runtime_identity = {'binary':str(binary),'purelib':str(purelib)}
    value.env = {'HOME':str(tmp_path / 'home'),'S08_ARCHIVE_SRC':str(value.source / 'src')}
    before = module.SourceProfile.fingerprint(value)
    value.env['S08_ARCHIVE_SRC'] = str(tmp_path / 'foreign-source')
    after = module.SourceProfile.fingerprint(value)
    assert before != after, 'loader/environment drift must invalidate the controlled profile'


def test_substituted_contract_manifest_is_refused_before_any_runtime_or_observer(tmp_path, monkeypatch):
    value = stub_profile(tmp_path)
    def substituted_snapshot(repo, commit, destination):
        fixture_dir = destination / 'contracts' / 'v1'
        fixture_dir.mkdir(parents=True)
        (fixture_dir / 'manifest.json').write_text('{}')
        return {'commit':commit}
    monkeypatch.setattr(module, '_snapshot', substituted_snapshot)
    monkeypatch.setattr(module, '_runtime', lambda *args: pytest.fail('unverified manifest reached runtime'))
    monkeypatch.setattr(module, 'Observer', lambda *args: pytest.fail('unverified manifest started observer'))
    with pytest.raises(ConformanceError):
        value.__enter__()


def test_fresh_profile_scrubs_parent_identity_and_isolates_tmp_xdg(tmp_path, monkeypatch):
    value = stub_profile(tmp_path)
    monkeypatch.setenv('HARFLOW_TASK_ID','parent-task-must-not-leak')
    monkeypatch.setenv('XDG_CONFIG_HOME','/foreign-config')
    monkeypatch.setenv('TMPDIR','/foreign-temp')
    monkeypatch.setattr(module,'_snapshot',lambda *args: {'commit':'a' * 40})
    monkeypatch.setattr(module,'verify_manifest',lambda *args: None)
    observed = []
    def runtime(python, output, env):
        observed.append(dict(env))
        raise RuntimeError('stop before any runtime operation')
    monkeypatch.setattr(module,'_runtime',runtime)
    with pytest.raises(RuntimeError,match='stop before'):
        value.__enter__()
    assert len(observed) == 1
    env = observed[0]
    assert 'HARFLOW_TASK_ID' not in env
    for key, name in [('HOME','home'),('TMPDIR','tmp'),('XDG_CONFIG_HOME','xdg-config'),
                      ('XDG_CACHE_HOME','xdg-cache'),('XDG_DATA_HOME','xdg-data')]:
        assert env[key] == str(value.output / name)
        assert Path(env[key]).is_dir()


def test_shared_controller_helper_drift_blocks_next_spawn(tmp_path, monkeypatch):
    value = stub_profile(tmp_path)
    value.source = tmp_path / 'source'
    value.runtime_identity.update(binary=str(tmp_path / 'python'),purelib=str(tmp_path / 'site'))
    monkeypatch.setattr(module,'tree_hash',lambda path:'unchanged-s08-and-profile-trees')
    monkeypatch.setattr(module,'read_regular',lambda path:b'unchanged-runtime-binary')
    helper_identity = {'process.py':'original-shared-helper'}
    monkeypatch.setattr(module,'framework_identity',lambda:dict(helper_identity))
    value.fingerprint = lambda: module.SourceProfile.fingerprint(value)
    value.initial_fingerprint = value.fingerprint()
    helper_identity['process.py'] = 'changed-shared-helper'
    monkeypatch.setattr(module,'run_process',lambda *args,**kwargs:pytest.fail('shared-helper drift spawned'))
    with pytest.raises(ConformanceError,match='fingerprint differs'):
        value._invoke(['version-info','--json'])
    assert value.records == []


@pytest.mark.parametrize('failure',['fingerprint','manifest-write'])
def test_setup_failure_after_observer_start_closes_once_and_retains_artifacts(tmp_path, monkeypatch, failure):
    value = module.SourceProfile(tmp_path,'a' * 40,tmp_path / 'output')
    events = []
    def snapshot(repo,commit,destination):
        destination.mkdir()
        return {'commit':commit}
    monkeypatch.setattr(module,'_snapshot',snapshot)
    monkeypatch.setattr(module,'verify_manifest',lambda *args:None)
    monkeypatch.setattr(module,'_runtime',lambda *args:{'binary':'stubbed','purelib':'stubbed'})
    monkeypatch.setattr(module,'framework_hash',lambda:'fixed-controller-identity')
    class Observer:
        socket_path = '/stubbed-not-connected'
        nonce = 'stubbed'
        def __init__(self,path):
            path.mkdir()
            (path / 'observer.jsonl').write_text('retained pre-task observation\n')
        def start(self):
            events.append('started')
            return self
        def cleanup(self,timeout):
            assert timeout == 5
            events.append('cleanup')
            return True
        def close(self): events.append('closed')
    monkeypatch.setattr(module,'Observer',Observer)
    def fingerprint(self):
        if failure == 'fingerprint':
            raise RuntimeError('independent fingerprint failure')
        return {'fixed':'identity'}
    monkeypatch.setattr(module.SourceProfile,'fingerprint',fingerprint)
    original_write = Path.write_text
    def write(path,*args,**kwargs):
        if failure == 'manifest-write' and path.name == 'injection-manifest.json':
            raise OSError('independent manifest-write failure')
        return original_write(path,*args,**kwargs)
    monkeypatch.setattr(Path,'write_text',write)
    with pytest.raises((RuntimeError,OSError),match=failure + ' failure'):
        value.__enter__()
    assert events == ['started','cleanup','closed']
    assert json.loads((value.output / 'cleanup.json').read_text()) == {'ok':True,'during_setup':True}
    assert (value.output / 'observer/observer.jsonl').read_text() == 'retained pre-task observation\n'
    assert (value.output / 'overlay/python-shim').is_file()
