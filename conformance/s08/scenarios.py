"""Independent S08 controlled-source scenarios, never stock-provider qualification.

Run only after independently testing the observer. The required preflight file and
caller-selected SHA bind external evidence; this runner does not attest its content.
All engine operations are public CLI subprocesses with a fresh profile handshake.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import time

from conformance.oracles import ConformanceError, read_regular, strict_json, validate_event_page
from conformance.process import run_process
from .profile import SourceProfile


def require(condition, message):
    if not condition:
        raise ConformanceError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


class Case:
    def __init__(self, profile):
        self.p = profile
        self.operations = []
        self.launches = []

    def call(self, args, *, refused=False):
        caps = ['ctl.v1']
        optional = {'task-events': 'task-event-page.v1', 'task-prompt': 'task-prompt.v1',
                    'send': 'messaging.v1', 'cancel-message': 'messaging.v1'}
        if args[0] in optional:
            caps.append(optional[args[0]])
        if args[0] == 'approval-respond' and '--monitor' in args:
            caps.append('caller-approvals.monitor-respond.v1')
        reply = self.p.command([*args, '--json'], required_capabilities=tuple(caps))
        doc = reply['document']
        self.operations.append({'argv': args, 'exit_code': reply['exit_code'],
            'document': doc, 'process': reply['process']})
        require((reply['exit_code'] != 0 and isinstance(doc.get('error'), dict)) if refused
                else (reply['exit_code'] == 0 and 'error' not in doc),
                'unexpected public refusal/success: ' + args[0])
        if args[0] == 'task-events' and not refused:
            validate_event_page(doc)
        return doc

    def init_git(self):
        # Fixture workspace only; never touch the checkout's refs or configuration.
        env = {**self.p.env, 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
               'GIT_CONFIG_SYSTEM': '/dev/null', 'GIT_TERMINAL_PROMPT': '0'}
        record = run_process(['/usr/bin/git', '-c', 'core.hooksPath=/dev/null', 'init',
                              str(self.p.workspace)], cwd=self.p.workspace, env=env,
                             artifact_dir=self.p.output / 'git-init', timeout=5)
        require(record['returncode'] == 0 and record['pipes_drained'] and
                not record['timed_out'] and not record['capture_exceeded'], 'fixture git init unknown')
        self.git = record

    def launch(self, mode='complete', *, flags=(), **spec):
        spec = dict(mode=mode, text='S08 exact result — 日本語 🧪\nsecond line', **spec)
        prompt = '  S08 Unicode café 日本語 🧪\nS08_FIXTURE_SPEC:' + json.dumps(spec, ensure_ascii=False)
        before = {r['token'] for r in self.p.observer.records}
        doc = self.call(['run', '--backend', 'conformance_fake', '--repo', str(self.p.workspace),
                         '--freedom', 'read_only', '--prompt', prompt, *flags])
        task_id = doc.get('result', {}).get('task_id')
        require(isinstance(task_id, str) and bool(task_id), 'launch lacks public task identity')
        require(self.p.observer.wait_for(lambda rows: any(r['role'] == 'fake' and
                r['token'] not in before for r in rows), 5), 'fake registration not observed')
        fakes = [r for r in self.p.observer.records if r['role'] == 'fake' and r['token'] not in before]
        require(len(fakes) == 1, 'launch must have one OS-observed fake')
        fake = fakes[0]
        parent = [r for r in self.p.observer.records if r['pid'] == fake['ppid'] and r['role'] == 'supervisor']
        require(len(parent) == 1, 'fake parent not corroborated supervisor')
        item = dict(task_id=task_id, prompt=prompt, spec=spec, fake=fake, supervisor=parent[0])
        self.launches.append(item)
        return item

    def resume(self, predecessor):
        spec = {'mode': 'complete', 'text': 'Explicit resume exact 日本語 🧪'}
        prompt = 'S08_FIXTURE_SPEC:' + json.dumps(spec, ensure_ascii=False)
        before = {r['token'] for r in self.p.observer.records}
        doc = self.call(['resume', predecessor['task_id'], prompt])
        require(self.p.observer.wait_for(lambda rows: any(r['role'] == 'fake' and
                r['token'] not in before for r in rows), 5), 'resume fake OS registration missing')
        fakes = [r for r in self.p.observer.records if r['role'] == 'fake' and r['token'] not in before]
        require(len(fakes) == 1, 'resume did not produce one fake')
        parents = [r for r in self.p.observer.records if r['pid'] == fakes[0]['ppid'] and r['role'] == 'supervisor']
        require(len(parents) == 1, 'resume supervisor ancestry missing')
        item = dict(task_id=doc['result']['task_id'], prompt=prompt, spec=spec, fake=fakes[0],
                    supervisor=parents[0], composed=True)
        self.launches.append(item)
        return item

    def status(self, task):
        return self.call(['status', task['task_id']])['task']

    def poll(self, operation, predicate, timeout=12):
        deadline = time.monotonic() + timeout
        for _ in range(40):
            value = operation()
            if predicate(value):
                return value
            if time.monotonic() >= deadline:
                break
        raise ConformanceError('bounded public observation predicate not reached')

    def terminal(self, task, expected='completed', exit_code=0):
        status = self.poll(lambda: self.status(task), lambda s: s.get('status') != 'running')
        require(status.get('status') == expected and status.get('exit_code') == exit_code,
                'terminal status/exit contradict expected scenario')
        require(status.get('output_drained') is True, 'terminal output not independently publicly drained')
        require(self.p.observer.wait_exited(task['fake']['token'], 5), 'fake OS exit not observed')
        page = self.call(['task-events', task['task_id'], '--from', 'oldest', '--all-kinds', '--limit', '200'])['result']
        require(page['log'].get('state') == 'final' and page['log'].get('output_drained') is True,
                'public final event capture not drained')
        require(any(e.get('kind') == 'task_finished' and e.get('status') == expected and
                    e.get('exit_code') == exit_code for e in page['events']), 'missing matching public finish event')
        return status

    def raw(self, task, *, completed=False):
        path = self.p.output / 'fake-ledger' / (str(task['fake']['pid']) + '.jsonl')
        data = read_regular(path)
        records = []
        for line in data.splitlines():
            row = strict_json(line)
            raw = bytes.fromhex(row['raw_hex'])
            require(len(raw) == row['bytes'] and sha(raw) == row['sha256'], 'fake raw byte/hash mismatch')
            records.append((row['direction'], strict_json(raw)))
        inputs = [v for d, v in records if d == 'stdin' and v.get('type') == 'input']
        require(bool(inputs) and task['prompt'] in inputs[0]['text'], 'raw initial assignment lost exact caller bytes')
        public = self.call(['task-prompt', task['task_id']])['result']['prompt']
        require(public.get('complete') is True and (public.get('text') == task['prompt'] if not task.get('composed') else
                isinstance(public.get('text'), str) and public['text'].endswith(task['prompt']) and
                public['text'] in inputs[0]['text']), 'public prompt differs from caller bytes')
        status = self.status(task)
        require(any(d == 'stdout' and v.get('subtype') == 'init' and
                    v.get('session_id') == status.get('session_id') for d, v in records), 'raw init/public session mismatch')
        if completed:
            require(any(d == 'stdout' and v.get('type') == 'result' and
                        v.get('result') == task['spec']['text'] for d, v in records), 'raw result missing exact expected text')
            require(status.get('summary') == task['spec']['text'], 'public result differs from raw expected text')
        return {'path': str(path), 'sha256': sha(data), 'bytes': len(data), 'untrusted': True,
                'corroborated_pid': task['fake']['pid'], 'frames': len(records)}

    def requests(self, task):
        return self.call(['approval-list', '--task-id', task['task_id']])['result']['requests']

    def pending(self, task, native):
        rows = self.poll(lambda: self.requests(task), lambda rows: any(
            r.get('native_request_id') == 's:' + native and r.get('decision') is None for r in rows))
        found = [r for r in rows if r.get('native_request_id') == 's:' + native and r.get('decision') is None]
        require(len(found) == 1, 'pending exact request ambiguous')
        return found[0]

    def answer(self, task, request, decision, *, monitor=True, refused=False, digest=None, revision=None):
        return self.call(['approval-respond', task['task_id'], request['request_id'],
            '--action-digest', request['action_digest'] if digest is None else digest,
            '--owner-revision', str(request['owner_revision'] if revision is None else revision),
            '--decision', decision, *(['--monitor'] if monitor else [])], refused=refused)


def refusals(c):
    before = c.p.observer.records
    c.call(['run', '--backend', 'conformance_fake', '--repo', str(c.p.workspace / 'missing'),
            '--prompt', 'must not spawn'], refused=True)
    c.call(['run', '--backend', 'conformance_fake', '--repo', str(c.p.workspace),
            '--prompt', 'must not spawn', '--max-turns', '1'], refused=True)
    require(not any(r['role'] == 'fake' for r in c.p.observer.records), 'prelaunch refusal spawned fake')
    require(not list((c.p.output / 'fake-ledger').iterdir()), 'prelaunch refusal wrote fake ledger')
    return {'initial_observer_records': len(before)}


def complete(c):
    t = c.launch()
    c.terminal(t)
    return c.raw(t, completed=True)


def nonzero(c):
    t = c.launch('nonzero')
    c.terminal(t, 'failed', 7)
    return c.raw(t, completed=True)


def detached(c):
    gate = c.p.workspace / 'release'
    t = c.launch('hold', gate=str(gate))
    require(c.status(t)['status'] == 'running', 'detached task prematurely terminal')
    require(any(r['role'] == 'cli' and r['exited'] for r in c.p.observer.records), 'initial CLI exit unobserved')
    require(not next(r for r in c.p.observer.records if r['token'] == t['supervisor']['token'])['exited'],
            'detached owner did not survive initial CLI')
    count = len([r for r in c.p.observer.records if r['role'] == 'fake'])
    c.call(['resume', t['task_id'], 'overlapping resume must refuse'], refused=True)
    require(len([r for r in c.p.observer.records if r['role'] == 'fake']) == count, 'busy resume spawned another fake')
    gate.write_bytes(b'caller-selected release\n')
    c.terminal(t)
    return c.raw(t, completed=True)


def cancelled(c):
    t = c.launch('hold', gate=str(c.p.workspace / 'never-release'))
    c.call(['cancel', t['task_id']])
    status = c.poll(lambda: c.status(t), lambda s: s.get('status') != 'running')
    require(status.get('status') == 'cancelled' and status.get('output_drained') is True,
            'cancel did not settle with drained cancelled outcome')
    require(type(status.get('exit_code')) is int and status['exit_code'] != 0, 'cancel exit unobserved or clean')
    c.terminal(t, 'cancelled', status['exit_code'])
    return c.raw(t)


def approvals(c):
    t = c.launch('approval', flags=('--caller-approvals', '--monitor'))
    sentinel = c.p.workspace / 'approved-effects.txt'
    a = c.pending(t, 'A')
    c.answer(t, a, 'approve', monitor=False, refused=True)
    c.answer(t, a, 'approve', digest='0' * 64, refused=True)
    c.answer(t, a, 'approve', revision=a['owner_revision'] + 1, refused=True)
    require(not sentinel.exists(), 'refused decision produced effects')
    c.answer(t, a, 'approve')
    b = c.pending(t, 'B')
    require(a['action_digest'] == b['action_digest'] and a['request_id'] != b['request_id'],
            'same action does not have distinct exact request identities')
    require(read_regular(sentinel) == b'A\n', 'approval A exact sentinel missing')
    c.answer(t, a, 'approve', refused=True)
    require(c.pending(t, 'B')['request_id'] == b['request_id'], 'duplicate A consumed B')
    c.answer(t, b, 'reject')
    c.terminal(t)
    require(read_regular(sentinel) == b'A\n', 'rejection B produced effects')
    return c.raw(t, completed=True)


def actor(c):
    target = c.launch('approval', flags=('--caller-approvals', '--monitor'))
    a = c.pending(target, 'A')
    child = c.launch('actor', target_task=target['task_id'])
    c.terminal(child)
    capture_dir = c.p.workspace / 'actor-capture'
    capture_bytes = read_regular(capture_dir / 'stdout.bin')
    ledger_bytes = read_regular(capture_dir / 'ledger.json')
    actor_ledger = strict_json(ledger_bytes)
    require(type(actor_ledger.get('returncode')) is int and actor_ledger['returncode'] != 0 and
            actor_ledger.get('pipes_drained') is True and actor_ledger.get('timed_out') is False and
            actor_ledger.get('capture_exceeded') is False and actor_ledger.get('cleanup') in
            ('group_signalled', 'group_absent', 'group_quiescent_os_observed'),
            'managed actor bounded refusal outcome unproved')
    require(actor_ledger.get('argv') == [str(c.p.shim), 'approval-list', '--task-id',
            target['task_id'], '--json'], 'managed actor captured operation identity mismatch')
    stream = actor_ledger.get('streams', {}).get('stdout', {})
    require(stream.get('path') == str(capture_dir / 'stdout.bin') and
            stream.get('sha256') == sha(capture_bytes) and
            stream.get('captured_bytes') == len(capture_bytes) and
            stream.get('bytes_seen') == len(capture_bytes), 'managed actor raw capture integrity mismatch')
    capture = strict_json(capture_bytes)
    require(capture.get('error', {}).get('code') == 'approval_error' and
            capture['error'].get('data', {}).get('reason') == 'unauthorized', 'managed actor authority did not refuse')
    require(c.p.observer.wait_for(lambda rows: any(r['role'] == 'cli' and
            r['pid'] == actor_ledger.get('pid') and r['ppid'] == child['fake']['pid'] and
            r['exited'] for r in rows), 5), 'exact managed actor CLI PID/exit/ancestry unproved')
    still_pending = c.pending(target, 'A')
    require(all(still_pending.get(key) == a.get(key) for key in
            ('request_id', 'action_digest', 'owner_revision', 'decision', 'native_request_id')),
            'managed actor mutated pending exact approval')
    require(not (c.p.workspace / 'approved-effects.txt').exists(), 'managed actor produced approval effects')
    c.answer(target, a, 'reject')
    c.answer(target, c.pending(target, 'B'), 'reject')
    c.terminal(target)
    return {'raw': [c.raw(child, completed=True), c.raw(target, completed=True)],
            'actor_capture': {'untrusted': True, 'ledger_sha256': sha(ledger_bytes),
                'stdout_sha256': sha(capture_bytes), 'stdout_bytes': len(capture_bytes),
                'corroborated_kernel_pid': actor_ledger['pid']}}


def messaging(c):
    gate = c.p.workspace / 'release'
    t = c.launch('hold', gate=str(gate))
    keep = '  queued café 日本語 🧪\nkeep exact trailing spaces  '
    withdrawn = 'cancelled unique Ω payload'
    immediate = 'now remains queued — λ\nexact second line'
    first = c.call(['send', t['task_id'], keep, '--delivery', 'queue'])['result']
    second = c.call(['send', t['task_id'], withdrawn, '--delivery', 'queue'])['result']
    third = c.call(['send', t['task_id'], immediate, '--delivery', 'now'])['result']
    require(all(x.get('status') == 'queued' for x in (first, second, third)), 'messages were not queued')
    require(third.get('refusal', {}).get('code') == 'interrupt_unsupported', 'now failed to refuse unsafe interruption')
    cancelled = c.call(['cancel-message', t['task_id'], second['message_id']])['result']
    require(cancelled.get('status') == 'cancelled', 'queued message cancellation failed')
    require(c.status(t)['status'] == 'running', 'messaging settled hold prematurely')
    require(len([r for r in c.p.observer.records if r['role'] == 'fake']) == 1, 'queue/now spawned a successor')
    gate.write_bytes(b'explicit release\n')
    c.terminal(t)
    ledger = read_regular(c.p.output / 'fake-ledger' / (str(t['fake']['pid']) + '.jsonl'))
    frames = [strict_json(bytes.fromhex(strict_json(line)['raw_hex'])) for line in ledger.splitlines()
              if strict_json(line)['direction'] == 'stdin']
    require(len(frames) == 1 and all(keep not in str(f) and immediate not in str(f) for f in frames),
            'queued message leaked into current live turn')
    successor = c.resume(t)
    c.terminal(successor)
    evidence = c.raw(successor, completed=True)
    raw = read_regular(c.p.output / 'fake-ledger' / (str(successor['fake']['pid']) + '.jsonl'))
    incoming = [strict_json(bytes.fromhex(strict_json(line)['raw_hex']))['text'] for line in raw.splitlines()
                if strict_json(line)['direction'] == 'stdin' and
                strict_json(bytes.fromhex(strict_json(line)['raw_hex'])).get('type') == 'input']
    require(len(incoming) == 1 and incoming[0].count(keep) == 1 and incoming[0].count(immediate) == 1
            and withdrawn not in incoming[0], 'explicit consume violated exact once/cancelled byte contract')
    items = c.status(t)['queued_followups']['items']
    require(any(i['id'] == second['message_id'] and i['status'] == 'cancelled' for i in items),
            'public cancelled metadata missing')
    for message in (first, third):
        require(any(i['id'] == message['message_id'] and i['status'] == 'consumed' and
                    i.get('successor_task_id') == successor['task_id'] for i in items),
                'public consumed metadata missing')
    # Explicitly resume the successor: inherited messages must not replay.
    again = c.resume(successor)
    c.terminal(again)
    again_raw = read_regular(c.p.output / 'fake-ledger' / (str(again['fake']['pid']) + '.jsonl'))
    for line in again_raw.splitlines():
        row = strict_json(line)
        if row['direction'] == 'stdin':
            frame = strict_json(bytes.fromhex(row['raw_hex']))
            require(keep not in frame.get('text', '') and immediate not in frame.get('text', '')
                    and withdrawn not in frame.get('text', ''), 'repeat resume replayed followups')
    return [c.raw(t, completed=True), evidence, c.raw(again, completed=True)]


def owner_death(c):
    t = c.launch('hold', gate=str(c.p.workspace / 'never-release'), survive_eof=True)
    require(c.p.observer.kill_participant(t['supervisor']['token']), 'verified owner self-termination unavailable')
    require(c.p.observer.wait_exited(t['supervisor']['token'], 5), 'owner OS exit unobserved')
    for _ in range(3):
        s = c.status(t)
        require(s.get('status') == 'running' and s.get('exit_code') is None and
                s.get('output_drained') is False, 'owner death fabricated terminal settlement')
        require(s.get('supervision', {}).get('capture_state') == 'lost_with_owner', 'owner loss not publicly reported')
        require(not next(r for r in c.p.observer.records if r['token'] == t['fake']['token'])['exited'],
                'owner-death fixture no longer alive')
    require(len([r for r in c.p.observer.records if r['role'] == 'fake']) == 1, 'read replayed a task')
    return c.raw(t)


SCENARIOS = {'refusals': refusals, 'complete': complete, 'nonzero': nonzero,
             'detached-resume': detached, 'cancel': cancelled, 'approvals': approvals,
             'managed-actor': actor, 'messaging': messaging, 'owner-death': owner_death}
UNSUPPORTED = ['PID reuse', 'host reboot',
               'disk full', 'full MCP restart', 'uncertain control families',
               'real backend/provider qualification', 'full C0 conformance']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--invocation-id', required=True, help='Caller-selected 32 lowercase hexadecimal characters')
    parser.add_argument('--repo', required=True, type=Path)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--python', required=True, type=Path)
    parser.add_argument('--observer-preflight', required=True, type=Path)
    parser.add_argument('--observer-preflight-sha256', required=True)
    parser.add_argument('--case', action='append', choices=tuple(SCENARIOS))
    args = parser.parse_args(argv)
    preflight = read_regular(args.observer_preflight)
    require(bool(preflight) and sha(preflight) == args.observer_preflight_sha256,
            'external observer preflight byte identity mismatch')
    require(re.fullmatch(r'[0-9a-f]{32}', args.invocation_id) is not None, 'invalid caller invocation identity')
    invocation = args.invocation_id
    root = Path('/private/tmp') / ('h8-' + invocation[:14])
    root.mkdir(mode=0o700, exist_ok=False)
    context = {'invocation_id': invocation, 'source_commit': args.commit,
               'observer_preflight_sha256': sha(preflight),
               'runner_sha256': sha(read_regular(Path(__file__)))}
    (root / 'context.json').write_text(json.dumps(context, indent=2, sort_keys=True))
    report = {**context, 'profile': 'controlled_source_fixture_injection',
        'stock_backend_qualification': False, 'full_C0': False, 'unsupported': UNSUPPORTED,
        'observer_gate': {'artifact': str(args.observer_preflight), 'sha256': sha(preflight),
            'interpretation': 'Caller supplied external preflight; content not self-attested by runner'}, 'cases': []}
    (root / 'observer-preflight.bin').write_bytes(preflight)
    for i, name in enumerate(args.case or SCENARIOS, 1):
        entry = {'name': name, 'output': str(root / ('case%02d' % i)), 'passed': False}
        case = None
        try:
            with SourceProfile(args.repo, args.commit, Path(entry['output']), args.python,
                               tasks_enabled=True) as profile:
                case = Case(profile)
                case.init_git()
                entry['evidence'] = SCENARIOS[name](case)
                entry['identity'] = profile.identity
            entry['passed'] = True
        except Exception as exc:
            entry['failure'] = {'type': type(exc).__name__, 'message': str(exc)[:1500],
                                'no_replay': True}
        finally:
            if case is not None:
                entry['operations'] = case.operations
                entry['launches'] = case.launches
                entry['observer_records'] = case.p.observer.records
                entry['git_init'] = getattr(case, 'git', None)
            report['cases'].append(entry)
            (root / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True))
        # Fail closed at the first defect. Do not start further engine scenarios.
        if not entry['passed']:
            report['not_run'] = [n for n in (args.case or SCENARIOS) if n not in [x['name'] for x in report['cases']]]
            break
    report['passed'] = all(e['passed'] for e in report['cases'])
    (root / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True))
    print(json.dumps({'report': str(root / 'report.json'), 'passed': report['passed']}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
