"""Private rendezvous with standalone owned children, never engine processes."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time

import pytest

from conformance.s09 import rendezvous as module

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(sys.platform!='darwin',reason='Darwin LOCAL_PEERPID')


@pytest.fixture
def setup(tmp_path,monkeypatch):
    folder=Path(tempfile.mkdtemp(prefix='s09-rv-',dir='/private/tmp'))
    shim=folder/'participant'
    shim.write_text('#!'+sys.executable+'\n'+'''import os,sys
from types import SimpleNamespace
from conformance.s09.rendezvous import acknowledge_owner
acknowledge_owner(SimpleNamespace(token='a'*32),sys.argv[4],sys.argv[6])
''')
    shim.chmod(0o700)
    monkeypatch.setenv('TMPDIR',str(folder))
    monkeypatch.setenv('CONFORMANCE_NONCE','independent123')
    monkeypatch.setenv('S08_PYTHON_SHIM',str(shim))
    monkeypatch.setenv('HARFLOW_STATE_ROOT',str(folder/'state'))
    # Child is a standalone protocol participant, with no engine import.
    monkeypatch.setenv('PYTHONPATH',str(ROOT))
    value=module.ParentRendezvous()
    yield value,shim,folder
    value.close()


def argv(shim,folder,run='owned-run'):
    return [str(shim),'-m','harflow_mcp.workflows','supervise',run,'--root',str(folder/'state')]


def finish(child):
    if child.poll() is None: child.kill()
    return child.communicate(timeout=4)


def test_real_popen_audit_intent_and_kernel_peer_ack_retained(setup):
    value,shim,folder=setup
    active=[True]
    def audit(event,args):
        if active[0]: value.audit(event,args)
    sys.addaudithook(audit)
    child=None
    try:
        assert Path(value.path).exists(), 'listener must predate real spawn'
        child=subprocess.Popen(argv(shim,folder),cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        value.wait()
        _,err=child.communicate(timeout=4)
        assert child.returncode==0,err
        rows=[json.loads(line) for line in (folder/'rendezvous-independent123.jsonl').read_text().splitlines()]
        assert [row['event'] for row in rows]==['intent','owner-acknowledged']
        assert rows[1]['peer_pid']==child.pid
        assert rows[1]['run_id']=='owned-run'
        assert all(row['parent_pid']==os.getpid() and row['command_nonce']=='independent123' for row in rows)
    finally:
        active[0]=False
        if child is not None: finish(child)


def test_idempotent_or_no_spawn_command_does_not_accept_or_wait(setup,monkeypatch):
    value,shim,folder=setup
    class NeverAccept:
        def accept(self): pytest.fail('no-spawn command waited for missing owner')
        def close(self): pass
    original=value.socket
    value.socket=NeverAccept()
    try: value.wait()
    finally: value.socket=original
    assert value.intents==[]


@pytest.mark.parametrize('change',[{'nonce':'wrong'},{'pid':1},{'pid':True},{'token':'short'},
    {'run_id':'foreign-run'},{'root':'foreign-root'}])
def test_nonce_pid_token_and_operation_mismatch_fail_without_ack(setup,change):
    value,shim,folder=setup
    value.audit('subprocess.Popen',(str(shim),argv(shim,folder),None,None))
    script='''import json,os,socket,sys
s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(2);s.connect(sys.argv[1])
f={'nonce':sys.argv[2],'pid':os.getpid(),'token':'a'*32,'run_id':'owned-run','root':sys.argv[3]}
f.update(json.loads(sys.argv[4]));s.sendall(json.dumps(f).encode()+b'\\n')
assert s.recv(4096)==b''
'''
    child=subprocess.Popen([sys.executable,'-c',script,value.path,value.nonce,str(folder/'state'),json.dumps(change)],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        with pytest.raises(ValueError,match='identity mismatch'): value.wait()
        _,err=child.communicate(timeout=3)
        assert child.returncode==0,err
        rows=(folder/'rendezvous-independent123.jsonl').read_text()
        assert 'owner-acknowledged' not in rows
    finally: finish(child)


def test_missing_owner_is_bounded_and_never_replays_spawn(setup,monkeypatch):
    value,shim,folder=setup
    value.audit('subprocess.Popen',(str(shim),argv(shim,folder),None,None))
    monkeypatch.setattr(module,'DEADLINE',.05)
    begin=time.monotonic()
    with pytest.raises(TimeoutError): value.wait()
    assert time.monotonic()-begin<1
    assert len(value.intents)==1


@pytest.mark.parametrize('raw',[b'[]\n',b'{broken}\n',b'x'*(module.LIMIT+1),b''])
def test_frame_shape_size_and_eof_are_fail_closed(raw):
    class Input:
        def __init__(self): self.raw=bytearray(raw)
        def settimeout(self,n): assert n>0
        def recv(self,n):
            if not self.raw:return b''
            return bytes([self.raw.pop(0)])
    with pytest.raises((ValueError,RuntimeError)):
        module.read_frame(Input(),time.monotonic()+1)


def test_kernel_peer_from_unregistered_ancestor_is_not_direct_owner(setup):
    value,shim,folder=setup
    value.audit('subprocess.Popen',(str(shim),argv(shim,folder),None,None))
    participant='''import json,os,socket,sys
s=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);s.settimeout(2);s.connect(sys.argv[1])
s.sendall(json.dumps({'nonce':sys.argv[2],'pid':os.getpid(),'token':'a'*32,'run_id':'owned-run','root':sys.argv[3]}).encode()+b'\\n')
assert s.recv(4096)==b''
'''
    intermediary='import subprocess,sys; r=subprocess.run([sys.executable,"-c",sys.argv[1],*sys.argv[2:]],timeout=3);sys.exit(r.returncode)'
    child=subprocess.Popen([sys.executable,'-c',intermediary,participant,value.path,value.nonce,str(folder/'state')],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    try:
        with pytest.raises(ValueError,match='parent|identity'): value.wait()
        _,err=child.communicate(timeout=4)
        assert child.returncode==0,err
        assert 'owner-acknowledged' not in (folder/'rendezvous-independent123.jsonl').read_text()
    finally: finish(child)


def test_all_exact_spawn_intents_need_separate_ack(setup):
    value,shim,folder=setup
    children=[]
    try:
        for run in ['first','second']:
            args=argv(shim,folder,run)
            value.audit('subprocess.Popen',(str(shim),args,None,None))
            children.append(subprocess.Popen(args,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE))
        value.wait()
        for child in children:
            _,err=child.communicate(timeout=3)
            assert child.returncode==0,err
        rows=[json.loads(line) for line in (folder/'rendezvous-independent123.jsonl').read_text().splitlines()]
        assert {row['run_id'] for row in rows if row['event']=='owner-acknowledged'}=={'first','second'}
    finally:
        for child in children:finish(child)


def test_rendezvous_bind_failure_closes_owned_socket(monkeypatch,tmp_path):
    closed=[]
    class Socket:
        def bind(self,path):raise OSError('independent allocation refusal')
        def close(self):closed.append(True)
    monkeypatch.setenv('TMPDIR','/private/tmp')
    monkeypatch.setattr(module.socket,'socket',lambda *a:Socket())
    with pytest.raises(OSError,match='allocation refusal'):module.ParentRendezvous()
    assert closed==[True]


def test_isolated_overlay_import_resolves_all_rendezvous_dependencies(tmp_path):
    import shutil
    from conformance.s09.profile import SourceProfile
    profile=SourceProfile(tmp_path,'a'*40,tmp_path/'out')
    overlay=tmp_path/'overlay'
    overlay.mkdir()
    for name,source in profile.overlay_sources().items():shutil.copyfile(source,overlay/name)
    script='import sys;sys.path.insert(0,sys.argv[1]);import rendezvous;assert callable(rendezvous.ancestry)'
    result=subprocess.run([sys.executable,'-I','-B','-c',script,str(overlay)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=4)
    assert result.returncode==0,result.stderr
