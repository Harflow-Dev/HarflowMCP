"""Fail-closed profile unit tests; all process boundaries are stubs."""
from pathlib import Path

import pytest

from conformance.oracles import ConformanceError
from conformance.s09 import profile as module
from conformance.s08 import profile as shared


def profile(tmp_path):
    value = module.SourceProfile(tmp_path, 'a'*40, tmp_path/'output', tasks_enabled=True)
    value.state_root, value.workspace, value.env = tmp_path/'state', tmp_path, {}
    value.runtime_identity = {'platform':'darwin','architecture':'arm64'}
    value.shim = tmp_path/'shim'
    value.initial_fingerprint = {'identity':'fixed'}
    value.fingerprint = lambda: dict(value.initial_fingerprint)
    return value


def handshake(root, capabilities=()):
    return dict(product='harflow',format='harflow.version-info',v=1,product_version='0.0.7',
        component='engine',contracts={k:dict(format=v,v=1) for k,v in
        [('ctl','harflow.ctl'),('setup','harflow.setup'),('events','harflow.event')]},
        capabilities=['ctl.v1','version-info.v1',*capabilities],os='darwin',architecture='arm64',state_root=str(root))


@pytest.mark.parametrize('argv', [['slot','service','--json'],['slot','enable','--json'],
    ['slot','migrate','--json'],['slot','cleanup','--json'],['scratch','remove','--json'],
    ['scratch','clean','--json'],['scratch','clean','--chain','x','--group','y','--json'],
    ['workflow-builder','--json'],['workflow-takeover','--json'],['install','--json']])
def test_unsafe_subcommands_refused_before_handshake(tmp_path, argv):
    value = profile(tmp_path)
    value._invoke = lambda argv: pytest.fail('unsafe command reached subprocess boundary')
    with pytest.raises(ConformanceError): value.command(argv)
    assert value.records == []


@pytest.mark.parametrize('argv', [['slot','status','heavy','--json'],['scratch','list','--json'],
    ['scratch','clean','--group','owned','--json'],['resources','--json'],
    ['statistics-page','--json'],['workflow-status','--run-id','owned','--json']])
def test_each_selected_operation_requires_fresh_capability_handshake(tmp_path, argv):
    value = profile(tmp_path)
    calls = []
    def invoke(args):
        calls.append(args)
        return {'document':handshake(value.state_root,['selected.v1'])}
    value._invoke = invoke
    for _ in range(2): value.command(argv, requiredcap='selected.v1')
    assert calls == [['version-info','--json'],argv,['version-info','--json'],argv]


def test_missing_capability_blocks_selected_operation(tmp_path):
    value = profile(tmp_path)
    calls = []
    value._invoke = lambda args: calls.append(args) or {'document':handshake(value.state_root)}
    with pytest.raises(ConformanceError):
        value.command(['statistics-page','--json'], requiredcap='statistics-page.v1')
    assert calls == [['version-info','--json']]


@pytest.mark.parametrize('fault',['timed_out','missing_returncode','unknown_cleanup','capture_exceeded','pipes_drained'])
def test_uncertain_mutation_is_retained_once_and_never_replayed(tmp_path,monkeypatch,fault):
    value = profile(tmp_path)
    calls = []
    def run(*args,**kwargs):
        calls.append(args)
        row = dict(timed_out=False,capture_exceeded=False,pipes_drained=True,
                   returncode=0,cleanup='group_quiescent_os_observed')
        if fault=='missing_returncode': row['returncode']=None
        elif fault=='unknown_cleanup': row['cleanup']='unknown'
        else: row[fault]=fault!='pipes_drained'
        return row
    monkeypatch.setattr(shared,'run_process',run)
    with pytest.raises(ConformanceError,match='never replay'):
        value._invoke(['workflow-start','owned','--json'])
    assert len(calls)==len(value.records)==1


def test_framework_identity_includes_s09_s08_and_shared_helpers(monkeypatch):
    paths=[]
    monkeypatch.setattr(module,'tree_hash',lambda path: paths.append(Path(path).name) or 'fixed')
    monkeypatch.setattr(module,'framework_identity',lambda:{'shared':'original'})
    original=module.framework_hash()
    assert set(paths)=={'s09','s08'}
    monkeypatch.setattr(module,'framework_identity',lambda:{'shared':'changed'})
    assert module.framework_hash()!=original


def test_case_configuration_canonical_copy_and_bound(tmp_path):
    source={'mode':'fixed'}
    value=module.SourceProfile(tmp_path,'a'*40,tmp_path/'out',case_spec=source)
    source['mode']='changed'
    assert value.case_spec=='{"mode": "fixed"}'
    with pytest.raises(ValueError): module.SourceProfile(tmp_path,'a'*40,tmp_path/'out',case_spec={'x':'a'*17000})
    with pytest.raises(ValueError): module.SourceProfile(tmp_path,'a'*40,tmp_path/'out',case_spec=['invalid'])


@pytest.mark.parametrize('args',[
    ['run','--workflow','owned'],['workflow-start','owned','--worktree'],
    ['run','--worktree'],['run','--worktree','--freedom','write_in_repo'],
    *[['run',flag,'owned'] for flag in ('--branch','--base','--fetch','--fetch-default','--fetch-required',
                                       '--worktree-base','--worktree-branch','--worktree-detach')],
])
def test_authorization_guards_precede_any_handshake(tmp_path,args):
    value=profile(tmp_path)
    value._invoke=lambda args:pytest.fail('branch/worktree or alternate workflow path reached invocation')
    with pytest.raises(ConformanceError):value.command([*args,'--json'])


def test_only_read_only_worktree_refusal_probe_is_permitted(tmp_path):
    value=profile(tmp_path)
    calls=[]
    value._invoke=lambda args:calls.append(args) or {'document':handshake(value.state_root)}
    args=['run','--freedom','read_only','--worktree','--json']
    value.command(args)
    assert calls==[['version-info','--json'],args]


@pytest.mark.parametrize('args',[
    ['run','--branch=owned'],['run','--worktree-branch=owned'],['run','--base=owned'],
    ['run','--bran=owned'],['run','--workf=owned'],
    ['run','--workflow=owned'],['workflow-start','owned','--worktree=true'],
    ['run','--worktree','--freedom=read_only','--freedom=write_in_repo'],
    ['run','--worktree','--freedom','read_only','--freedom','write_in_repo'],
    ['run','--worktree','--freedom=read_only','--freedom','read_only'],
])
def test_equal_option_and_duplicate_freedom_cannot_bypass_authorization(tmp_path,args):
    value=profile(tmp_path)
    value._invoke=lambda args:pytest.fail('authorization option bypass reached invocation')
    with pytest.raises(ConformanceError):value.command([*args,'--json'])


def test_equal_read_only_freedom_allows_owned_refusal_probe(tmp_path):
    value=profile(tmp_path)
    calls=[]
    value._invoke=lambda args:calls.append(args) or {'document':handshake(value.state_root)}
    args=['run','--freedom=read_only','--worktree','--json']
    value.command(args)
    assert calls==[['version-info','--json'],args]


def test_abbreviated_freedom_cannot_override_explicit_read_only_probe(tmp_path):
    value=profile(tmp_path)
    value._invoke=lambda args:pytest.fail('abbreviated freedom reached invocation')
    with pytest.raises(ConformanceError):value.command(['run','--worktree','--freedom','read_only','--free=write_in_repo','--json'])
