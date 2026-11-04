"""Independent negative oracle probes with no engine task execution."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from conformance.oracles import ConformanceError
from conformance.s08 import scenarios as module


class ObserverStub:
    def __init__(self):
        self.records = []
        self.exit_observed = True
    def wait_for(self, predicate, timeout): return predicate(self.records)
    def wait_exited(self, token, timeout): return self.exit_observed


def case(tmp_path):
    observer = ObserverStub()
    profile = SimpleNamespace(observer=observer,workspace=tmp_path,output=tmp_path)
    return module.Case(profile)


def fake(token='fake-one', parent=200):
    return dict(role='fake',token=token,pid=300,ppid=parent,exited=False)


def supervisor():
    return dict(role='supervisor',token='supervisor-one',pid=200,ppid=100,exited=False)


@pytest.mark.parametrize('observed', [[], [fake(parent=999)], [fake(),fake('fake-two'),supervisor()],
                                     [dict(fake(),role='unregistered')]])
def test_launch_claim_cannot_replace_exact_os_fake_and_parent(tmp_path, observed):
    # pytest supplies tmp_path and the deliberately invalid external observations.
    c = case(tmp_path)
    def call(args):
        c.p.observer.records = observed
        return {'result': {'task_id':'public-task'}}
    c.call = call
    with pytest.raises(ConformanceError):
        c.launch()
    assert c.launches == []


def terminal_case(tmp_path):
    c = case(tmp_path)
    task = dict(task_id='public-task',fake=fake())
    status = dict(status='completed',exit_code=0,output_drained=True)
    event = dict(kind='task_finished',status='completed',exit_code=0)
    page = {'result':{'log':{'state':'final','output_drained':True},'events':[event]}}
    c.status = lambda task: status
    c.call = lambda args: page
    return c, task, status, page


@pytest.mark.parametrize('fault', ['exit_unknown','public_exit_missing','public_status_wrong',
                                  'public_drain_missing','event_not_final','event_not_drained','finish_missing'])
def test_terminal_fake_claims_cannot_replace_public_and_os_settlement(tmp_path, fault):
    c, task, status, page = terminal_case(tmp_path)
    if fault == 'exit_unknown': c.p.observer.exit_observed = False
    elif fault == 'public_exit_missing': status['exit_code'] = None
    elif fault == 'public_status_wrong': status['status'] = 'failed'
    elif fault == 'public_drain_missing': status.pop('output_drained')
    elif fault == 'event_not_final': page['result']['log']['state'] = 'open'
    elif fault == 'event_not_drained': page['result']['log']['output_drained'] = False
    elif fault == 'finish_missing': page['result']['events'] = []
    with pytest.raises(ConformanceError):
        c.terminal(task)


def raw_case(tmp_path):
    c = case(tmp_path)
    task = dict(task_id='public-task',fake=fake(),prompt=' exact caller 日本語\n',spec={'text':'exact result'})
    frames = [('stdin',{'type':'input','text':'system prefix ' + task['prompt']}),
              ('stdout',{'type':'system','subtype':'init','session_id':'owned-session'}),
              ('stdout',{'type':'result','result':'exact result'})]
    records = []
    for direction, frame in frames:
        raw = json.dumps(frame).encode() + b'\n'
        records.append(dict(direction=direction,raw_hex=raw.hex(),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
    directory = tmp_path / 'fake-ledger'
    directory.mkdir()
    path = directory / '300.jsonl'
    public = {'complete':True,'text':task['prompt']}
    status = {'session_id':'owned-session','summary':'exact result'}
    c.call = lambda args: {'result':{'prompt':public}}
    c.status = lambda task: status
    return c,task,records,path,public,status


def write_rows(path, records):
    path.write_text(''.join(json.dumps(row) + '\n' for row in records))


def test_raw_claims_remain_explicitly_untrusted_even_when_byte_consistent(tmp_path):
    c,task,records,path,public,status = raw_case(tmp_path)
    write_rows(path,records)
    evidence = c.raw(task,completed=True)
    assert evidence['untrusted'] is True
    assert evidence['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    # raw() checks content consistency; launch()/terminal() supply separate OS proof.
    assert c.p.observer.records == []


@pytest.mark.parametrize('fault', ['raw_hash','raw_length','raw_initial_prompt','public_prompt',
                                  'public_incomplete','public_session','raw_result','public_result'])
def test_raw_bytes_and_public_prompt_session_result_must_agree(tmp_path, fault):
    c,task,records,path,public,status = raw_case(tmp_path)
    if fault == 'raw_hash': records[0]['sha256'] = '0' * 64
    elif fault == 'raw_length': records[0]['bytes'] += 1
    elif fault == 'raw_initial_prompt': task['prompt'] = 'different exact caller'
    elif fault == 'public_prompt': public['text'] = 'changed caller'
    elif fault == 'public_incomplete': public['complete'] = False
    elif fault == 'public_session': status['session_id'] = 'foreign-session'
    elif fault == 'raw_result': records.pop()
    elif fault == 'public_result': status['summary'] = 'changed result'
    write_rows(path,records)
    with pytest.raises(ConformanceError):
        c.raw(task,completed=True)


def cli_args(preflight):
    return ['--invocation-id','0123456789abcdef0123456789abcdef','--repo','/never-entered','--commit','a' * 40,'--python','/never-executed',
            '--observer-preflight',str(preflight),'--observer-preflight-sha256',
            hashlib.sha256(preflight.read_bytes()).hexdigest(),'--case','complete']


def test_preflight_identity_mismatch_blocks_profile_before_launch(tmp_path, monkeypatch):
    preflight = tmp_path / 'preflight'
    preflight.write_bytes(b'independent observer evidence')
    args = cli_args(preflight)
    args[args.index('--observer-preflight-sha256') + 1] = '0' * 64
    monkeypatch.setattr(module,'SourceProfile',lambda *args,**kwargs: pytest.fail('invalid gate entered profile'))
    with pytest.raises(ConformanceError,match='preflight byte identity mismatch'):
        module.main(args)


def test_existing_invocation_directory_is_never_clobbered(tmp_path, monkeypatch):
    preflight = tmp_path / 'preflight'
    preflight.write_bytes(b'independent observer evidence')
    # A patched Path redirects only the fixed runner output root into this test.
    original_path = Path
    def redirected(value):
        return tmp_path if value == '/private/tmp' else original_path(value)
    monkeypatch.setattr(module,'Path',redirected)
    occupied = tmp_path / 'h8-0123456789abcd'
    occupied.mkdir()
    sentinel = occupied / 'report.json'
    sentinel.write_bytes(b'prior retained evidence')
    monkeypatch.setattr(module,'SourceProfile',lambda *args,**kwargs: pytest.fail('collision entered profile'))
    with pytest.raises(FileExistsError):
        module.main(cli_args(preflight))
    assert sentinel.read_bytes() == b'prior retained evidence'


@pytest.mark.parametrize('identity', ['short','A' * 32,'../' + 'a' * 29])
def test_invalid_caller_invocation_identity_never_enters_profile(tmp_path, monkeypatch, identity):
    preflight = tmp_path / 'preflight'
    preflight.write_bytes(b'caller-selected gate evidence')
    args = cli_args(preflight)
    args[1] = identity
    monkeypatch.setattr(module,'SourceProfile',lambda *args,**kwargs: pytest.fail('bad identity entered profile'))
    with pytest.raises(ConformanceError,match='invalid caller invocation identity'):
        module.main(args)


def test_external_full_identity_and_gate_context_exist_before_profile_entry(tmp_path, monkeypatch):
    preflight = tmp_path / 'preflight'
    preflight.write_bytes(b'caller-selected gate evidence')
    original_path = Path
    monkeypatch.setattr(module,'Path',lambda value: tmp_path if value == '/private/tmp' else original_path(value))
    calls = []
    def denied_profile(*args,**kwargs):
        root = tmp_path / 'h8-0123456789abcd'
        context = json.loads((root / 'context.json').read_text())
        assert context['invocation_id'] == '0123456789abcdef0123456789abcdef'
        assert context['source_commit'] == 'a' * 40
        assert context['observer_preflight_sha256'] == hashlib.sha256(preflight.read_bytes()).hexdigest()
        assert (root / 'observer-preflight.bin').read_bytes() == preflight.read_bytes()
        calls.append(context)
        raise RuntimeError('independent pre-task stop')
    monkeypatch.setattr(module,'SourceProfile',denied_profile)
    assert module.main(cli_args(preflight)) == 1
    assert len(calls) == 1
    report = json.loads((tmp_path / 'h8-0123456789abcd/report.json').read_text())
    assert report['passed'] is False
    assert report['cases'][0]['failure']['no_replay'] is True
    assert report['observer_gate']['interpretation'] == 'Caller supplied external preflight; content not self-attested by runner'


def test_launch_accepts_one_fresh_os_fake_with_correlated_supervisor_and_public_identity(tmp_path):
    c = case(tmp_path)
    def call(args):
        c.p.observer.records = [fake(),supervisor()]
        return {'result':{'task_id':'public-task'}}
    c.call = call
    observed = c.launch()
    assert observed['task_id'] == 'public-task'
    assert observed['fake']['ppid'] == observed['supervisor']['pid']
    assert c.launches == [observed]
