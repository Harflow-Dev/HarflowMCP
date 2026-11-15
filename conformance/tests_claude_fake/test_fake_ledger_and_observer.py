"""Self-checks for the fake's evidence ledger and its registration with the OS observer (Darwin for the observer part)."""
import hashlib
import json
import os
from pathlib import Path
import re
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import time

import pytest

import fakekit
from conformance.lifecycle import observer as observer_module
from conformance.lifecycle.seams import ConformanceError, ledger_bytes, read_ledger
from fakekit import SESSION, Rec, frame, live, marker, read_rows

darwin = pytest.mark.skipif(sys.platform != 'darwin', reason='the lifecycle observer needs Darwin kqueue and LOCAL_PEERPID; Linux lifecycle is unavailable')


# --- ledger content -------------------------------------------------------------------------------------------

def test_agent_ledger_records_start_io_mode_and_exit_with_verifiable_digests(fake):
    stdin = frame('hello')
    code, lines, err, raw = fake.run(live(), fake.cwd, stdin=stdin)
    assert code == 0
    [path] = fakekit.ledgers(fake.ledger_dir)
    assert re.fullmatch(r'\d+-[0-9a-f]{16}\.jsonl', path.name)
    ledger = read_ledger(path)  # re-verifies every raw row against its length and digest
    start, rows = ledger['start'], ledger['rows']
    assert start['kind'] == 'agent' and start['argv'] == live() and start['session_id'] == SESSION and start['session_flag'] == '--session-id'
    assert path.name.startswith(f'{start["pid"]}-') and start['ppid'] == os.getpid() and start['cwd'] == fake.real_cwd
    assert start['bundle_sha256'] == hashlib.sha256((fakekit.BUNDLE / 'SHA256SUMS').read_bytes()).hexdigest()
    assert start['fake_sha256'] == hashlib.sha256((fake.fake / 'fake_claude.py').read_bytes()).hexdigest()
    assert start['registered'] is False and not [r for r in rows if r['event'] == 'registration'], 'no observer configured: nothing was registered'
    assert ledger_bytes(rows, 'stdin') == stdin and ledger_bytes(rows, 'stdout') == raw and ledger_bytes(rows, 'stderr') == b''
    mode = next(r for r in rows if r['event'] == 'mode')
    assert mode['mode'] == 'complete' and mode['scripted'] is False and mode['recording'] == 'r02_live_single'
    assert ledger['complete'] and ledger['exit']['code'] == 0 and ledger['digest_only'] == 0
    assert rows.index(start) == 0 and [r['event'] for r in rows].count('exit') == 1
    for row in rows:
        if row['event'] == 'io' and row['direction'] != 'stdin':
            assert row['written'] == row['bytes'] and 'error' not in row, 'a write record holds the length actually written'


def test_scripted_modes_are_marked_scripted_in_the_ledger(fake):
    for spec, scripted in ((marker(mode='nonzero'), True), (marker(mode='tool_use'), False)):
        fake.run(live(), fake.cwd, stdin=frame(spec))
    flags = {}
    for path in fakekit.ledgers(fake.ledger_dir):
        mode = next(r for r in read_rows(path) if r['event'] == 'mode')
        flags[mode['mode']] = mode['scripted']
    assert flags == {'nonzero': True, 'tool_use': False}


def test_a_ledger_without_an_exit_entry_is_incomplete_not_malformed(fake):
    proc = fake.spawn(live(), fake.cwd)
    proc.send(frame(marker(mode='hold', gate=str(fake.work / 'gate'))))
    proc.read_lines(Rec('r02_live_single').first_assistant_split())
    proc.p.kill()
    proc.p.wait(timeout=30)
    [path] = fakekit.ledgers(fake.ledger_dir)
    ledger = read_ledger(path)
    assert ledger['complete'] is False and ledger['exit'] is None and ledger['start']['kind'] == 'agent'


def test_a_partial_trailing_line_is_ignored_and_a_tampered_row_is_detected(fake, tmp_path):
    fake.run(live(), fake.cwd, stdin=frame('x'))
    [path] = fakekit.ledgers(fake.ledger_dir)
    original = path.read_bytes()
    copy = tmp_path / 'partial.jsonl'
    copy.write_bytes(original + b'{"event": "io", "dire')
    assert read_ledger(copy)['complete'] is True
    rows = original.decode().splitlines()
    index = next(i for i, line in enumerate(rows) if '"direction": "stdout"' in line)
    row = json.loads(rows[index])
    row['raw_hex'] = row['raw_hex'][:-2] + ('00' if row['raw_hex'][-2:] != '00' else '01')
    rows[index] = json.dumps(row)
    forged = tmp_path / 'forged.jsonl'
    forged.write_text('\n'.join(rows) + '\n')
    with pytest.raises(ConformanceError, match='byte/digest mismatch'):
        read_ledger(forged)
    row = json.loads(original.decode().splitlines()[index])
    row['bytes'] += 1
    rows[index] = json.dumps(row)
    forged.write_text('\n'.join(rows) + '\n')
    with pytest.raises(ConformanceError, match='byte/digest mismatch'):
        read_ledger(forged)


def test_after_the_raw_budget_records_are_digest_only_and_never_verified_raw(fake):
    frames = [frame('turn %d' % i) for i in range(220)]
    code, lines, _, raw = fake.run(live(), fake.cwd, stdin=b''.join(frames), timeout=240)
    assert code == 0 and len(raw) > 1024 * 1024
    [path] = fakekit.ledgers(fake.ledger_dir)
    rows = read_rows(path)
    io = [r for r in rows if r['event'] == 'io']
    raw_rows, digest_only = [r for r in io if 'raw_hex' in r], [r for r in io if r.get('digest_only')]
    assert digest_only and all('raw_hex' not in r and r['bytes'] > 0 and re.fullmatch('[0-9a-f]{64}', r['sha256']) for r in digest_only)
    assert sum(r['bytes'] for r in raw_rows) <= 1024 * 1024
    # the first record to overflow the budget is the first digest-only record; nothing before it is digest-only
    first = io.index(digest_only[0])
    assert all('raw_hex' in r for r in io[:first])
    ledger = read_ledger(path)
    assert ledger['digest_only'] == len(digest_only) and ledger['complete']
    # digest-only writes still carry the true digest of what was written, line by line
    written = [hashlib.sha256(line + b'\n').hexdigest() for line in raw.split(b'\n') if line]
    assert [r['sha256'] for r in io if r['direction'] == 'stdout'] == written
    assert len(ledger_bytes(rows, 'stdout')) < len(raw), 'part of the stream is only digested'


# --- the observer ---------------------------------------------------------------------------------------------

@pytest.fixture
def short_dir():
    path = Path(tempfile.mkdtemp(prefix='cfo'))
    yield path
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def observed(fake, short_dir):
    observer = observer_module.OsObserver(short_dir / 'o').start()
    fake.set(observer_socket=observer.socket_path, nonce=observer.nonce)
    yield observer
    fake.cleanup()
    fake.stop_file.write_bytes(b'')
    observer.close()


def wait_for(predicate, timeout=60):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise AssertionError('condition not reached in time')


def ledger_rows(fake, event=None):
    rows = [row for path in fakekit.ledgers(fake.ledger_dir) for row in read_rows(path)]
    return [r for r in rows if event is None or r['event'] == event]


@darwin
def test_version_probe_and_refused_argv_never_register_but_an_agent_registers_once(fake, observed):
    assert fake.run_raw(['--version'], fake.cwd)[0] == 0
    assert fake.run([*live(), '--bogus'], fake.cwd)[0] == 1
    assert fake.run([*live(), '--permission-prompt-tool', 'x'], fake.cwd)[0] == 1
    assert observed.records == [] and not observed.failed
    kinds = sorted(r['kind'] for r in ledger_rows(fake, 'start'))
    assert kinds == ['refused', 'refused']
    code, *_ = fake.run(live(), fake.cwd, stdin=frame('x'))
    assert code == 0
    assert [r['role'] for r in observed.records] == ['fake'], 'probe then launch gives exactly one registration'
    [agent] = [r for r in ledger_rows(fake, 'start') if r['kind'] == 'agent']
    [registration] = ledger_rows(fake, 'registration')
    assert registration['ok'] is True and registration['ack_token'] == observed.records[0]['token'] and agent['pid'] == observed.records[0]['pid']
    assert registration['lstart'] == observed.records[0]['lstart'] and registration['ppid'] == observed.records[0]['ppid']
    record = observed.records[0]
    assert [p.name for p in fakekit.ledgers(fake.ledger_dir) if p.name.startswith(f'{record["pid"]}-')], 'the ledger is named after the registered pid'


@darwin
def test_registration_precedes_output_and_the_fake_is_observed_exiting(fake, observed):
    code, *_ = fake.run(live(), fake.cwd, stdin=frame('x'))
    [record] = observed.records
    assert code == 0 and observed.wait_exited(record['pid'], record['lstart'], 10)
    assert record['ppid'] == os.getpid()


@darwin
def test_escape_descendant_registers_as_descendant_in_its_own_session_and_exits_at_the_gate(fake, observed):
    gate = fake.work / 'gate'
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd, parent=True)
    proc.send(frame(marker(mode='escape', gate=str(gate), survive_eof=True)))
    proc.read_lines(split)
    wait_for(lambda: len(observed.records) == 2)
    by_role = {r['role']: r for r in observed.records}
    assert sorted(by_role) == ['descendant', 'fake']
    fake_record, descendant = by_role['fake'], by_role['descendant']
    assert descendant['ppid'] == fake_record['pid'] and descendant['pgid'] == descendant['pid'] != fake_record['pgid']
    assert [r['pid'] for r in ledger_rows(fake, 'descendant')] == [descendant['pid']]
    assert not observed.exited(descendant['pid'], descendant['lstart'])
    gate.write_bytes(b'')
    code, rest, _ = proc.finish(close=False)
    assert code == 0 and rest == Rec('r02_live_single').expect(split, None, session=SESSION, cwd=fake.real_cwd)
    assert observed.wait_exited(descendant['pid'], descendant['lstart'], 10) and observed.wait_exited(fake_record['pid'], fake_record['lstart'], 10)
    assert not observed.failed


@darwin
def test_escape_descendant_has_its_own_lifetime_bound_and_releases_pipes_unless_asked(fake, observed):
    fake.set(observer_socket=observed.socket_path, nonce=observed.nonce, lifetime=4)
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd, parent=True)
    proc.send(frame(marker(mode='escape', gate=str(fake.work / 'never'), survive_eof=True)))
    proc.read_lines(split)
    wait_for(lambda: len(observed.records) == 2)
    fake_record = next(r for r in observed.records if r['role'] == 'fake')
    descendant = next(r for r in observed.records if r['role'] == 'descendant')
    os.kill(fake_record['pid'], signal.SIGKILL)
    assert observed.wait_exited(fake_record['pid'], fake_record['lstart'], 10)
    # the descendant is on its own alarm: it ends by itself without the gate, the stop file or its parent
    assert observed.wait_exited(descendant['pid'], descendant['lstart'], 20), 'the escaped descendant must honour its own lifetime bound'
    proc.p.wait(timeout=30)


@darwin
def test_descendant_that_holds_the_pipes_keeps_stdout_open_after_the_fake_dies(fake, observed):
    fake.set(observer_socket=observed.socket_path, nonce=observed.nonce, lifetime=6)
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd, parent=False)
    proc.send(frame(marker(mode='escape', gate=str(fake.work / 'never'), survive_eof=True, descendant_holds_pipes=True)))
    proc.read_lines(split)
    wait_for(lambda: len(observed.records) == 2)
    proc.p.kill()
    proc.p.wait(timeout=30)
    fd = proc.p.stdout.fileno()
    assert not select.select([fd], [], [], 1.5)[0], 'the holding descendant keeps the pipe open past its parent'
    descendant = next(r for r in observed.records if r['role'] == 'descendant')
    assert observed.wait_exited(descendant['pid'], descendant['lstart'], 20)
    assert select.select([fd], [], [], 5)[0] and os.read(fd, 1024) == b''


@darwin
def test_stop_file_releases_the_fake_and_its_descendant(fake, observed):
    split = Rec('r02_live_single').first_assistant_split()
    proc = fake.spawn(live(), fake.cwd, parent=True)
    proc.send(frame(marker(mode='escape', gate=str(fake.work / 'never'), survive_eof=True)))
    proc.read_lines(split)
    wait_for(lambda: len(observed.records) == 2)
    fake.stop_file.write_bytes(b'')
    assert proc.finish(close=False)[0] == 0
    for record in observed.records:
        assert observed.wait_exited(record['pid'], record['lstart'], 10)


# --- kill-owner -----------------------------------------------------------------------------------------------

def start_holding(fake, observed, **spec):
    proc = fake.spawn(live(), fake.cwd, parent=True)
    proc.send(frame(marker(mode='hold', gate=str(fake.work / 'never'), survive_eof=True, **spec)))
    proc.read_lines(Rec('r02_live_single').first_assistant_split())
    wait_for(lambda: observed.records)
    return proc, observed.records[0]


@darwin
def test_kill_owner_ends_only_the_recorded_parent_and_logs_the_result(fake, observed):
    proc, record = start_holding(fake, observed)
    bystander = subprocess.Popen(['/bin/sleep', '60'])
    try:
        assert record['chain'][0]['pid'] == proc.pid == record['ppid']
        # the request body is ignored: a pid written into it signals nothing
        (fake.control_dir / (record['token'] + '.kill-owner')).write_text(str(bystander.pid))
        row = wait_for(lambda: ledger_rows(fake, 'kill-owner'))[0]
        assert row == {**row, 'requested': True, 'sent': True}
        assert proc.p.wait(timeout=30) == -signal.SIGKILL
        assert observed.wait_exited(proc.pid, record['chain'][0]['lstart'], 10)
        assert bystander.poll() is None, 'nothing but the recorded parent was signalled'
        assert not observed.exited(record['pid'], record['lstart']), 'the fake itself survives its owner'
    finally:
        bystander.kill()
        bystander.wait()


@darwin
def test_a_request_for_another_token_sends_nothing(fake, observed):
    proc, record = start_holding(fake, observed)
    (fake.control_dir / (('0' * 32) + '.kill-owner')).write_bytes(b'')
    (fake.control_dir / (record['token'][:-1] + '.kill-owner')).write_bytes(b'')
    (fake.control_dir / (record['token'] + '.kill-owner.tmp')).write_bytes(b'')
    time.sleep(1.5)
    assert ledger_rows(fake, 'kill-owner') == [] and proc.alive()


@darwin
def test_after_the_parent_changed_kill_owner_signals_nothing(fake, observed):
    proc, record = start_holding(fake, observed)
    proc.p.kill()
    proc.p.wait(timeout=30)
    wait_for(lambda: os.getppid() and subprocess.run(['/bin/ps', '-o', 'ppid=', '-p', str(record['pid'])], capture_output=True).stdout.strip() == b'1')
    (fake.control_dir / (record['token'] + '.kill-owner')).write_bytes(b'')
    row = wait_for(lambda: ledger_rows(fake, 'kill-owner'))[0]
    assert row['requested'] is True and row['sent'] is False
    assert not observed.exited(record['pid'], record['lstart'])
    # it is answered once: a second request file changes nothing further
    time.sleep(.5)
    assert len(ledger_rows(fake, 'kill-owner')) == 1


def test_participant_kill_parent_refuses_a_forged_or_changed_ppid():
    from conformance.lifecycle import participant
    assert participant.kill_parent({'ppid': os.getppid() + 100000}) is False
    assert participant.kill_parent({'ppid': 1}) is False and participant.kill_parent({'ppid': 0}) is False


# --- configuration and registration failures ---------------------------------------------------------------------

@pytest.mark.parametrize('lifetime', [0, -1, 601, '60', 1.5, True])
def test_fake_json_lifetime_must_be_a_bounded_integer(fake, lifetime):
    fake.configure(lifetime=lifetime)
    code, _, err, raw = fake.run(live(), fake.cwd, stdin=frame('x'))
    assert code == 70 and raw == b'' and b'lifetime' in err


def test_unreadable_fake_json_exits_70_before_anything_else(fake):
    (fake.fake / 'fake.json').write_text('{not json')
    code, _, err, raw = fake.run(live(), fake.cwd, stdin=frame('x'))
    assert code == 70 and raw == b'' and b'fake.json' in err
    (fake.fake / 'fake.json').write_text('[]')
    assert fake.run(live(), fake.cwd, stdin=frame('x'))[0] == 70


def test_an_unreachable_observer_fails_closed_with_a_ledger_and_no_output(fake):
    fake.set(observer_socket=str(fake.work / 'no-such.sock'), nonce='0' * 32)
    code, _, err, raw = fake.run(live(), fake.cwd, stdin=frame('x'))
    assert code == 70 and raw == b'' and b'observer registration failed' in err
    [path] = fakekit.ledgers(fake.ledger_dir)
    rows = read_rows(path)
    assert rows[0]['kind'] == 'agent' and rows[0]['registered'] is False
    assert any(r['event'] == 'registration' and r['ok'] is False for r in rows) and rows[-1]['code'] == 70


@darwin
def test_a_wrong_nonce_is_refused_by_the_observer_and_the_fake_exits_70(fake, observed):
    fake.set(observer_socket=observed.socket_path, nonce='f' * 32)
    code, _, err, raw = fake.run(live(), fake.cwd, stdin=frame('x'))
    assert code == 70 and raw == b'' and b'observer registration failed' in err
    assert observed.records == [] and observed.failed
