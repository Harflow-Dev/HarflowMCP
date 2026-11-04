"""S09 gate: only owned standalone Python processes; no engine invocation."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest

from conformance.s08.observer import Observer

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='Darwin kernel observer only')


def ledger(observer):
    return [json.loads(line) for line in (observer.directory / 'observer.jsonl').read_text().splitlines()]


def start(observer, script):
    return subprocess.Popen([sys.executable, '-u', '-c', script, observer.socket_path, observer.nonce],
                            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def finish(child):
    if child.poll() is None:
        child.kill()  # Only this test's unreaped direct child.
    return child.communicate(timeout=4)


CHAIN = r'''
import subprocess,sys
from conformance.s08.client import register
path,nonce=sys.argv[1:3]
i=int(sys.argv[3]) if len(sys.argv)>3 else 0
role=('cli','workflow-owner','fake')[i]
h=register(path,nonce,role,control=i!=0,source_profile='s09')
if i<2:
    script=sys.argv[4] if len(sys.argv)>4 else SCRIPT
    c=subprocess.Popen([sys.executable,'-u','-c',script,path,nonce,str(i+1),script])
    assert c.wait(timeout=8)==0
h.close()
'''
CHAIN = 'SCRIPT=' + repr(CHAIN) + '\n' + CHAIN


def test_workflow_owner_requires_opt_in_before_ack():
    parent = Path(tempfile.mkdtemp(prefix='s09-gate-', dir='/private/tmp'))
    observer = Observer(parent / 'observer', source_profile='s09').start()
    script = '''
import sys
from conformance.s08.client import register
h=register(sys.argv[1],sys.argv[2],'cli',control=False,source_profile='s09')
import subprocess
c=subprocess.run([sys.executable,'-c',"from conformance.s08.client import register; import sys; register(sys.argv[1],sys.argv[2],'workflow-owner',source_profile='s09')",sys.argv[1],sys.argv[2]],timeout=4)
assert c.returncode != 0
h.close()
'''
    child = start(observer, script)
    try:
        _, err = child.communicate(timeout=6)
        assert child.returncode == 0, err
        assert [row['role'] for row in observer.records] == ['cli']
        assert any(row['event'] == 'registration-refused' for row in ledger(observer))
        assert observer.cleanup(.5) is False
    finally:
        finish(child)
        observer.close()


def test_opted_in_workflow_owner_and_fake_have_exact_kernel_parent_and_exit():
    parent = Path(tempfile.mkdtemp(prefix='s09-gate-', dir='/private/tmp'))
    observer = Observer(parent / 'observer', source_profile='s09', allow_workflows=True).start()
    child = start(observer, CHAIN)
    try:
        _, err = child.communicate(timeout=10)
        assert child.returncode == 0, err
        assert observer.wait_roles({'cli','workflow-owner','fake'}, 2), ledger(observer)
        records = {row['role']:row for row in observer.records}
        assert records['cli']['pid'] == child.pid
        assert records['cli']['ppid'] == os.getpid()
        assert records['workflow-owner']['ppid'] == child.pid
        assert records['fake']['ppid'] == records['workflow-owner']['pid']
        assert all(observer.wait_exited(row['token'], 2) for row in records.values())
        assert observer.cleanup(1) is True
        assert all(any(event['event']=='exit' and event['token']==row['token'] for event in ledger(observer)) for row in records.values())
    finally:
        observer.cleanup(1)
        finish(child)
        observer.close()


@pytest.mark.parametrize('role', ['workflow-owner','fake','supervisor'])
def test_workflow_opt_in_never_relaxes_unregistered_parent(role):
    parent = Path(tempfile.mkdtemp(prefix='s09-gate-', dir='/private/tmp'))
    observer = Observer(parent / 'observer', source_profile='s09', allow_workflows=True).start()
    child = start(observer, "from conformance.s08.client import register; import sys; register(sys.argv[1],sys.argv[2]," + repr(role) + ",source_profile='s09')")
    try:
        child.communicate(timeout=4)
        assert child.returncode != 0
        assert observer.records == []
        assert observer.cleanup(.2) is False
    finally:
        finish(child)
        observer.close()


def chain_for(roles):
    script = r"""
import subprocess,sys
from conformance.s08.client import register
path,nonce=sys.argv[1:3]
i=int(sys.argv[3]) if len(sys.argv)>3 else 0
role=ROLES[i]
h=register(path,nonce,role,control=i!=0,source_profile='s09')
if i<len(ROLES)-1:
    script=sys.argv[4] if len(sys.argv)>4 else SCRIPT
    c=subprocess.Popen([sys.executable,'-u','-c',script,path,nonce,str(i+1),script])
    assert c.wait(timeout=8)==0
h.close()
"""
    script='ROLES='+repr(roles)+'\n'+script
    return 'SCRIPT='+repr(script)+'\n'+script


def test_workflow_owned_task_supervisor_preserves_exact_full_os_chain():
    parent=Path(tempfile.mkdtemp(prefix='s09-gate-',dir='/private/tmp'))
    observer=Observer(parent/'observer',source_profile='s09',allow_workflows=True).start()
    roles=('cli','workflow-owner','supervisor','fake')
    child=start(observer,chain_for(roles))
    try:
        _,err=child.communicate(timeout=10)
        assert child.returncode==0,err
        assert observer.wait_roles(set(roles),2),ledger(observer)
        records={row['role']:row for row in observer.records}
        assert len(records)==4
        assert records['cli']['pid']==child.pid
        previous=os.getpid()
        for role in roles:
            assert records[role]['ppid']==previous
            previous=records[role]['pid']
            assert observer.wait_exited(records[role]['token'],2),ledger(observer)
        assert observer.cleanup(1) is True
    finally:
        observer.cleanup(1)
        finish(child)
        observer.close()


def test_registered_fake_cannot_be_parent_of_task_supervisor():
    parent=Path(tempfile.mkdtemp(prefix='s09-gate-',dir='/private/tmp'))
    observer=Observer(parent/'observer',source_profile='s09',allow_workflows=True).start()
    child=start(observer,chain_for(('cli','workflow-owner','fake','supervisor')))
    try:
        child.communicate(timeout=10)
        assert child.returncode!=0
        assert observer.wait_roles({'cli','workflow-owner','fake'},2),ledger(observer)
        assert {row['role'] for row in observer.records}=={'cli','workflow-owner','fake'}
        # Public wait helpers stop early on any refusal; retain separate bounded
        # kernel exit observation for the already registered owned chain.
        with observer._condition:
            assert observer._condition.wait_for(lambda: all(row['exited'] for row in observer.records),timeout=2)
        assert any(row['event']=='registration-refused' for row in ledger(observer))
        assert observer.cleanup(.2) is False
    finally:
        finish(child)
        observer.close()
