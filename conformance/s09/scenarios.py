"""Bounded S09 public CLI matrix; passing cases do not imply complete C0 acceptance."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import time

from conformance.oracles import ConformanceError, read_regular, strict_json, validate_event_page
from conformance.s08.scenarios import Case as TaskCase
from .oracles import require, workflow_terminal, correlate_managed, workflow_input_matches
from .profile import SourceProfile, framework_hash

SOURCE = 'b0185be855fd13acc94cb2d2008400d285d15df2'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for child in value.values():
            yield from strings(child)
    elif isinstance(value, list):
        for child in value:
            yield from strings(child)


def definition(name, worker=None):
    worker = worker or {'mode': 'complete', 'text': 'S09 exact worker café 日本語 🧪'}
    return {'name': name, 'routing_mode': 'explicit',
        'orchestrator': {'backend': 'conformance_fake', 'fallbacks': []},
        'max_parallel': 2, 'max_transitions': 24, 'max_decision_attempts': 2,
        'nodes': [{'id': 'start', 'type': 'start', 'position': {'x': 0, 'y': 0}},
            {'id': 'work', 'type': 'agent', 'role': 'task', 'purpose': 'Bounded synthetic worker',
             'instructions': 'S09_WORKER_SPEC:' + json.dumps(worker, ensure_ascii=False),
             'agent': {'backend': 'conformance_fake', 'fallbacks': []}, 'freedom': 'read_only',
             'execution_mode': 'headless', 'session_mode': 'fresh', 'max_attempts': 1,
             'position': {'x': 200, 'y': 0}},
            {'id': 'end', 'type': 'end', 'position': {'x': 400, 'y': 0}}],
        'connections': [{'id': 'begin', 'source': 'start', 'target': 'work'},
                        {'id': 'finish', 'source': 'work', 'target': 'end'}]}


class Case(TaskCase):
    def __init__(self, profile):
        super().__init__(profile)
        self.accepted = []
        self.workflow_runs = []
        self._assigned = set()
        self._before = {}

    def call(self, args, *, refused=False, code=None):
        caps = ['ctl.v1']
        optional = {'task-events': 'task-event-page.v1', 'task-prompt': 'task-prompt.v1',
                    'send': 'messaging.v1', 'cancel-message': 'messaging.v1',
                    'slot': 'phase-slots.v1'}
        if args[0] in optional:
            caps.append(optional[args[0]])
        if args[0] == 'statistics-page':
            caps.extend(('statistics-page.v1', 'statistics-cost-model.v1'))
        if args[0] == 'workflow-inspect' and '--view' in args and args[args.index('--view') + 1] == 'activity':
            caps.append('workflow-live-activity.v1')
        if '--attention-checkpoint-id' in args:
            caps.append('workflow-attention-checkpoints.v1')
        reply = self.p.command([*args, '--json'], required_capabilities=tuple(caps))
        doc = reply['document']
        self.operations.append({'argv': list(args), 'exit_code': reply['exit_code'],
            'document': doc, 'process': reply['process']})
        require((reply['exit_code'] != 0 and isinstance(doc.get('error'), dict)) if refused else
                (reply['exit_code'] == 0 and 'error' not in doc), 'unexpected public outcome: ' + args[0])
        if code is not None:
            require(doc.get('error', {}).get('code') == code, 'public refusal code differs: ' + args[0])
        if args[0] == 'task-events' and not refused:
            validate_event_page(doc)
        return doc

    def poll(self, operation, predicate, timeout=25):
        deadline = time.monotonic() + timeout
        for _ in range(40):
            result = operation()
            if predicate(result):
                return result
            if time.monotonic() >= deadline:
                break
        raise ConformanceError('bounded public observation predicate not reached')

    def accept(self, mode='complete', *, flags=(), **spec):
        spec = {'mode': mode, 'text': 'S09 exact result café 日本語 🧪\n  spaces', **spec}
        prompt = '  S09 caller prose Ω\nS08_FIXTURE_SPEC:' + json.dumps(spec, ensure_ascii=False)
        before = {row['token'] for row in self.p.observer.records}
        result = self.call(['run', '--backend', 'conformance_fake', '--repo', str(self.p.workspace),
                            '--freedom', 'read_only', '--prompt', prompt, *flags])['result']
        require(isinstance(result.get('task_id'), str), 'accepted task identity missing')
        item = {'task_id': result['task_id'], 'prompt': prompt, 'spec': spec}
        self.accepted.append(item)
        self._before[item['task_id']] = before
        return item

    def attach(self, task):
        before = self._before[task['task_id']]
        def candidates(rows):
            return [row for row in rows if row['role'] == 'fake' and row['token'] not in before
                    and row['token'] not in self._assigned]
        require(self.p.observer.wait_for(lambda rows: bool(candidates(rows)), 5), 'task fake OS spawn missing')
        found = candidates(self.p.observer.records)
        require(len(found) == 1, 'task fake association ambiguous')
        fake = found[0]
        parents = [row for row in self.p.observer.records if row['pid'] == fake['ppid'] and row['role'] == 'supervisor']
        require(len(parents) == 1, 'task supervisor ancestry missing')
        task.update(fake=fake, supervisor=parents[0])
        self._assigned.add(fake['token'])
        self.launches.append(task)
        return task

    def launch(self, mode='complete', *, flags=(), **spec):
        return self.attach(self.accept(mode, flags=flags, **spec))

    def frames(self, fake):
        path = self.p.output / 'fake-ledger' / (str(fake['pid']) + '.jsonl')
        data = read_regular(path)
        require(len(data) <= 1048576, 'fake raw ledger exceeded declared bound')
        frames = []
        for line in data.splitlines():
            row = strict_json(line)
            raw = bytes.fromhex(row['raw_hex'])
            require(type(row.get('bytes')) is int and len(raw) == row['bytes'] and
                    sha(raw) == row.get('sha256') and row.get('direction') in ('stdin', 'stdout'),
                    'untrusted raw fixture ledger inconsistent')
            frames.append((row['direction'], strict_json(raw)))
        require(bool(frames), 'fake raw frames absent')
        return frames, {'pid': fake['pid'], 'path': str(path), 'sha256': sha(data),
                        'bytes': len(data), 'untrusted': True}

    def save(self, value):
        path = self.p.workspace / (value['name'] + '.json')
        path.write_text(json.dumps(value, ensure_ascii=False))
        valid = self.call(['workflow-validate', '--definition', str(path)])['result']
        require(valid.get('valid') is True, 'authored workflow invalid: ' + str(valid.get('error')))
        saved = self.call(['workflow-save', value['name'], '--definition', str(path)])['result']
        require(saved.get('name') == value['name'] and isinstance(saved.get('workflow_id'), str),
                'saved definition identity missing')
        return saved

    def start(self, name, *, prompt='  S09 original task Ω\nexact spaces  '):
        self.call(['workflow-preview', name, '--repo', str(self.p.workspace), '--qualification-only'])
        run = self.call(['workflow-start', name, '--repo', str(self.p.workspace), '--prompt', prompt, '--monitor'])['result']
        require(isinstance(run.get('workflow_run_id'), str), 'workflow launch identity missing')
        self.workflow_runs.append(run['workflow_run_id'])
        return run['workflow_run_id']

    def workflow_status(self, run_id):
        run = self.call(['workflow-status', run_id])['result']
        require(run.get('workflow_run_id') == run_id and isinstance(run.get('status'), str),
                'public workflow status identity missing')
        return run

    def tree(self, run_id):
        records, pending = {}, [run_id]
        while pending:
            current = pending.pop(0)
            if current in records:
                continue
            require(len(records) < 8, 'bounded workflow tree exceeded')
            run = self.workflow_status(current)
            records[current] = run
            def visit(value):
                if isinstance(value, dict):
                    for key, child in value.items():
                        if key == 'child_workflow_run_id' and isinstance(child, str):
                            require(child != current, 'self-linked workflow')
                            if child not in records:
                                pending.append(child)
                        else:
                            visit(child)
                elif isinstance(value, list):
                    for child in value:
                        visit(child)
            visit(run.get('activations', []))
        for ident, child in records.items():
            if ident != run_id:
                link = child.get('parent_link', {})
                require(link.get('root_workflow_run_id') == run_id and link.get('workflow_run_id') in records,
                        'nested run reciprocal public ownership missing')
        return records

    def workflow_evidence(self, run_id, expected='completed'):
        self.poll(lambda: self.workflow_status(run_id), lambda run: run.get('status') == expected
                  and run.get('settling') is False)
        require(self.p.observer.wait_for(lambda rows: bool([r for r in rows if r['role'] == 'workflow-owner']) and
                all(r['exited'] for r in rows if r['role'] in ('workflow-owner', 'supervisor', 'fake')), 6),
                'workflow owner/task process exits not observed')
        runs = self.tree(run_id)
        fakes = [row for row in self.p.observer.records if row['role'] == 'fake']
        workflow_terminal(runs[run_id], fakes, expected)
        raw = {row['token']: self.frames(row) for row in fakes}
        matched, tasks = set(), []
        for run in runs.values():
            for activation in run.get('activations', []):
                for attempt in activation.get('tasks', []):
                    tid = attempt.get('task_id')
                    require(isinstance(tid, str), 'workflow attempt task identity missing')
                    status = self.call(['status', tid])['task']
                    require(attempt.get('status') == status.get('status'), 'workflow attempt/task public status mismatch')
                    prompt = self.call(['task-prompt', tid])['result']['prompt']
                    require(prompt.get('complete') is True, 'workflow task prompt incomplete')
                    if 'assignment_prompt' in attempt:
                        require(prompt.get('text') == attempt['assignment_prompt'], 'public task/workflow audit prompts disagree')
                    if activation.get('role') == 'orchestrator':
                        require(isinstance(run.get('prompt'), str) and
                                attempt.get('context_delivery', {}).get('scope') == run['workflow_run_id'],
                                'workflow control original request or public scope missing')
                    choices = []
                    for fake in fakes:
                        frames = raw[fake['token']][0]
                        if workflow_input_matches(frames, status.get('session_id'), prompt['text'],
                                                  activation.get('role'), attempt.get('context_delivery'),
                                                  original_request=run.get('prompt')):
                            choices.append(fake)
                    require(len(choices) == 1 and choices[0]['token'] not in matched,
                            'workflow task lacks unique OS/raw/public prompt/session association')
                    fake = choices[0]
                    matched.add(fake['token'])
                    wanted = 'completed' if expected == 'completed' else status.get('status')
                    observed_exit = status.get('exit_code')
                    require(wanted in ('completed', 'cancelled') and type(observed_exit) is int and
                            (observed_exit == 0 if wanted == 'completed' else observed_exit != 0),
                            'workflow attempt outcome contradicts expected fixture')
                    self.terminal({'task_id': tid, 'fake': fake}, wanted, observed_exit)
                    tasks.append({'task_id': tid, 'node_id': activation.get('node_id'), 'role': activation.get('role'),
                        'run_id': run['workflow_run_id'], 'status': wanted, 'fake_pid': fake['pid'],
                        'session_id': status.get('session_id'), 'raw': raw[fake['token']][1]})
        require(matched == {fake['token'] for fake in fakes}, 'unassociated workflow fake spawn')
        if expected == 'cancelled':
            require(any(task['status'] == 'cancelled' for task in tasks), 'workflow cancellation lacks cancelled child')
        return {'runs': list(runs), 'tasks': tasks, 'owner_pids': [r['pid'] for r in self.p.observer.records if r['role'] == 'workflow-owner']}

    def managed_evidence(self, fake, argv, *, code, message=None):
        directory = self.p.workspace / ('actor-' + str(fake['pid'])) / 'capture'
        ledger_bytes = read_regular(directory / 'ledger.json')
        stdout = read_regular(directory / 'stdout.bin')
        ledger = strict_json(ledger_bytes)
        require(ledger.get('streams', {}).get('stdout', {}).get('path') == str(directory / 'stdout.bin'),
                'managed capture path not owned')
        document = correlate_managed(self.p.observer.records, ledger, [str(self.p.shim), *argv], fake['pid'], stdout)
        require(document['error'].get('code') == code, 'managed refusal did not reach expected authority gate')
        if message is not None:
            require(document['error'].get('message') == message, 'managed refusal has unrelated diagnostic')
        return {'untrusted': True, 'kernel_pid': ledger['pid'], 'ledger_sha256': sha(ledger_bytes),
                'stdout_sha256': sha(stdout), 'error_code': document['error']['code']}


def definitions(c):
    value = definition('s09-definition')
    saved = c.save(value)
    got = c.call(['workflow-get', value['name']])['result']
    require(got == saved, 'saved definition round trip changed')
    listed = c.call(['workflow-list'])['result']
    require(any(value['name'] == text for text in strings(listed)), 'saved workflow absent from public listing')
    path = c.p.workspace / (value['name'] + '.json')
    c.call(['workflow-save', value['name'], '--definition', str(path), '--expected-revision', str(saved['revision'] + 1)], refused=True)
    require(c.call(['workflow-get', value['name']])['result'] == saved, 'stale definition save changed data')
    c.call(['workflow-start', value['name'], '--repo', str(c.p.workspace), '--prompt', 'must refuse caller access override',
            '--freedom', 'read_only', '--monitor'], refused=True)
    c.call(['workflow-delete', value['name']])
    c.call(['workflow-get', value['name']], refused=True)
    require(not any(row['role'] in ('fake', 'workflow-owner') for row in c.p.observer.records),
            'definition/refusal case launched execution')
    return {'workflow_id': saved['workflow_id'], 'revision': saved['revision']}


def workflow_complete(c):
    c.save(definition('s09-complete', {'mode': 'actor', 'operation': 'workflow-status', 'target_run': 'self',
                                     'text': 'Managed node exact result Ω'}))
    rid = c.start('s09-complete')
    evidence = c.workflow_evidence(rid)
    require(c.workflow_status(rid).get('prompt') == '  S09 original task Ω\nexact spaces  ',
            'workflow original caller prose changed')
    workers = [item for item in evidence['tasks'] if item['role'] == 'node']
    require(len(workers) == 1, 'sequential workflow worker count differs')
    fake = next(row for row in c.p.observer.records if row['pid'] == workers[0]['fake_pid'])
    evidence['managed'] = c.managed_evidence(fake, ['workflow-status', rid, '--json'], code='workflow_error',
                                          message='Worker nodes cannot inspect workflow context')
    require(len(evidence['owner_pids']) == 1, 'normal workflow owner unexpectedly replaced')
    return evidence


def workflow_input(c):
    c.save(definition('s09-input'))
    rid = c.start('s09-input')
    waiting = c.poll(lambda: c.workflow_status(rid), lambda run: run.get('status') == 'needs_input')
    decision = waiting.get('input_decision_id')
    require(isinstance(decision, str) and bool(decision), 'pending input decision identity missing')
    count = len([row for row in c.p.observer.records if row['role'] == 'fake'])
    c.call(['workflow-resume', rid, '--monitor', '--decision-id', 'not-the-issued-decision',
            '--instructions', 'must not deliver'], refused=True)
    require(c.workflow_status(rid).get('input_decision_id') == decision and
            len([row for row in c.p.observer.records if row['role'] == 'fake']) == count,
            'stale input response changed checkpoint or spawned work')
    answer = '  S09_AUTHORED_ANSWER Ω\nkeep exact trailing spaces  '
    c.call(['workflow-resume', rid, '--monitor', '--decision-id', decision, '--instructions', answer])
    evidence = c.workflow_evidence(rid)
    c.call(['workflow-resume', rid, '--monitor', '--decision-id', decision, '--instructions', answer], refused=True)
    final = c.workflow_status(rid)
    require(any(answer in value for value in strings(final)), 'accepted caller prose not retained exactly')
    require(any(answer in value for item in evidence['tasks'] if item['role'] == 'orchestrator'
                for direction, frame in c.frames(next(row for row in c.p.observer.records if row['pid'] == item['fake_pid']))[0]
                if direction == 'stdin' and frame.get('type') == 'input'
                for value in strings(strict_json(frame['text'].encode()))),
            'caller answer absent from actual orchestrator input')
    evidence['input_decision_id'] = decision
    return evidence


def parallel_nested(c):
    child = c.save(definition('s09-child'))
    parent = definition('s09-parallel')
    held = parent['nodes'][1]
    held['id'] = 'held'
    held['instructions'] = 'S09_WORKER_SPEC:' + json.dumps({'mode': 'hold', 'gate': str(c.p.workspace / 'parallel-release'),
        'survive_eof': True, 'text': 'Parallel branch exact result λ'})
    child_node = {'id': 'child', 'type': 'workflow', 'purpose': 'Nested bounded workflow',
        'workflow_ref': {'workflow_id': child['workflow_id']}, 'orchestrator_mode': 'child',
        'child_session_policy': 'fresh', 'instructions': 'Run the saved child fixture exactly',
        'max_attempts': 1, 'position': {'x': 400, 'y': 200}}
    parent['nodes'] = [parent['nodes'][0],
        {'id': 'split', 'type': 'parallel_start', 'parallel_group_id': 'group', 'branch_selection': 'all', 'position': {'x': 100, 'y': 0}},
        held, child_node,
        {'id': 'join', 'type': 'parallel_end', 'parallel_group_id': 'group', 'position': {'x': 600, 'y': 0}},
        parent['nodes'][-1]]
    parent['connections'] = [{'id': str(i), 'source': a, 'target': b} for i, (a, b) in enumerate(
        (('start', 'split'), ('split', 'held'), ('split', 'child'), ('held', 'join'), ('child', 'join'), ('join', 'end')))]
    c.save(parent)
    rid = c.start('s09-parallel')
    def branches_ready(run):
        activities = run.get('activations', [])
        return any(a.get('node_id') == 'held' and any(t.get('status') == 'running' for t in a.get('tasks', [])) for a in activities) and any(
            a.get('node_id') == 'child' and a.get('status') == 'completed' for a in activities)
    active = c.poll(lambda: c.workflow_status(rid), branches_ready, timeout=45)
    require(active['status'] == 'running' and active.get('settling') is True, 'parallel join did not retain active branch')
    require(len([row for row in c.p.observer.records if row['role'] == 'workflow-owner']) == 1,
            'nested workflow unexpectedly launched a separate owner')
    active_tree = c.tree(rid)
    require(len(active_tree) == 2 and all(run['status'] == 'completed' and run.get('settling') is False
            for ident, run in active_tree.items() if ident != rid), 'nested child did not finish while sibling held')
    held_tasks = [task['task_id'] for activation in active['activations'] if activation.get('node_id') == 'held'
                  for task in activation.get('tasks', [])]
    require(len(held_tasks) == 1, 'held branch task ambiguous')
    held_status = c.call(['status', held_tasks[0]])['task']
    live_fakes = [row for row in c.p.observer.records if row['role'] == 'fake' and not row['exited']]
    require(len(live_fakes) == 1 and held_status.get('supervision', {}).get('process_state') == 'alive' and
            any(direction == 'stdout' and frame.get('subtype') == 'init' and frame.get('session_id') == held_status.get('session_id')
                for direction, frame in c.frames(live_fakes[0])[0]), 'held parallel branch OS/public session association missing')
    (c.p.workspace / 'parallel-release').write_bytes(b'explicit parallel release\n')
    evidence = c.workflow_evidence(rid)
    require(len(evidence['runs']) == 2 and len(evidence['owner_pids']) == 1,
            'nested public run identities do not share exactly one observed owner')
    nodes = {(item['run_id'], item['node_id']) for item in evidence['tasks'] if item['role'] == 'node'}
    require((rid, 'held') in nodes and any(run != rid and node == 'work' for run, node in nodes),
            'parallel/nested worker executions missing')
    evidence['active_branch_sequence'] = active.get('sequence')
    evidence['held_fake_pid_before_release'] = live_fakes[0]['pid']
    # Public deletion reports references; existing runs retain their pinned tree.
    # Missing dependencies block new parent runs rather than prohibiting deletion.
    pinned = c.workflow_status(rid)['dependency_tree']
    deleted = c.call(['workflow-delete', 's09-child'])['result']
    require(deleted.get('deleted') == 's09-child' and 's09-parallel' in deleted.get('referenced_by', []),
            'referenced definition deletion omitted public referrers')
    owners_before = len([row for row in c.p.observer.records if row['role'] == 'workflow-owner'])
    fakes_before = len([row for row in c.p.observer.records if row['role'] == 'fake'])
    refused = c.call(['workflow-start', 's09-parallel', '--repo', str(c.p.workspace), '--prompt',
                     'Missing dependency must refuse before execution', '--monitor'], refused=True)
    require(any(child['workflow_id'] in text for text in strings(refused['error'])),
            'new run refusal did not identify the missing dependency')
    retained = c.workflow_status(rid)
    require(retained.get('status') == 'completed' and retained.get('dependency_tree') == pinned and
            len([row for row in c.p.observer.records if row['role'] == 'workflow-owner']) == owners_before and
            len([row for row in c.p.observer.records if row['role'] == 'fake']) == fakes_before,
            'definition deletion changed pinned run or dispatched a missing dependency')
    evidence['deleted_reference'] = deleted
    return evidence


def workflow_cancel_recover(c):
    gate = c.p.workspace / 'workflow-never-release'
    c.save(definition('s09-recovery', {'mode': 'hold', 'gate': str(gate), 'survive_eof': True,
                                    'text': 'Must be cancelled before this result'}))
    rid = c.start('s09-recovery')
    def active_work(run):
        return next((a for a in run.get('activations', []) if a.get('node_id') == 'work'
                     and a.get('role') == 'node' and any(t.get('status') == 'running' for t in a.get('tasks', []))), None)
    active = c.poll(lambda: c.workflow_status(rid), lambda run: bool(active_work(run)))
    issued = active_work(active)
    task_ids = [task['task_id'] for task in issued['tasks']]
    require(len(task_ids) == 1, 'recovery fixture worker identity ambiguous')
    require(c.p.observer.wait_for(lambda rows: any(row['role'] == 'fake' and not row['exited'] for row in rows), 5),
            'recovery worker OS registration missing')
    owners = [row for row in c.p.observer.records if row['role'] == 'workflow-owner']
    require(len(owners) == 1 and not owners[0]['exited'], 'recovery setup owner not live')
    fake_count = len([row for row in c.p.observer.records if row['role'] == 'fake'])
    require(c.p.observer.kill_participant(owners[0]['token']) and c.p.observer.wait_exited(owners[0]['token'], 5),
            'workflow owner exact self-termination/exit unproved')
    lost = []
    for _ in range(2):
        run = c.workflow_status(rid)
        status = c.call(['status', task_ids[0]])['task']
        require(status.get('status') == 'running' and status.get('exit_code') is None and
                status.get('output_drained') is False, 'scheduler death fabricated child settlement')
        require(len([row for row in c.p.observer.records if row['role'] == 'workflow-owner']) == 1 and
                len([row for row in c.p.observer.records if row['role'] == 'fake']) == fake_count,
                'read relaunched scheduler or worker')
        lost.append(run)
    token = lost[-1].get('attention_checkpoint_id')
    require(isinstance(token, str) and token.startswith('scheduler:'), 'qualified scheduler recovery token missing')
    c.call(['workflow-recover', rid, '--monitor', '--reason', 'must not apply stale recovery',
            '--attention-checkpoint-id', 'scheduler:not-current'], refused=True, code='attention_checkpoint_stale')
    c.call(['workflow-recover', rid, '--monitor', '--reason', 'Inspected isolated scheduler death; preserve issued work',
            '--attention-checkpoint-id', token])
    require(c.p.observer.wait_for(lambda rows: len([row for row in rows if row['role'] == 'workflow-owner']) == 2, 5),
            'explicit replacement scheduler spawn unobserved')
    recovered = c.workflow_status(rid)
    current = active_work(recovered)
    require(current is not None and current['id'] == issued['id'] and
            [task['task_id'] for task in current['tasks']] == task_ids and
            len([row for row in c.p.observer.records if row['role'] == 'fake']) == fake_count,
            'explicit recovery replayed or replaced the issued worker')
    c.call(['workflow-cancel', rid, '--monitor'])
    evidence = c.workflow_evidence(rid, 'cancelled')
    require(not gate.exists(), 'cancelled workflow fixture gate released')
    evidence.update(recovered_task_ids=task_ids, scheduler_checkpoint=token,
                    lost_observations=lost, recovered_sequence=recovered.get('sequence'))
    return evidence


from .resource_scenarios import resources_fifo, resource_owner_loss, scratch_phase_refusals
from .statistics_scenarios import statistics

SCENARIOS = {'definitions': definitions, 'workflow-complete': workflow_complete,
    'parallel-nested': parallel_nested, 'workflow-input': workflow_input,
    'workflow-cancel-recover': workflow_cancel_recover, 'resources-fifo': resources_fifo,
    'resource-owner-loss': resource_owner_loss, 'scratch-phase-refusals': scratch_phase_refusals,
    'statistics': statistics}

GAPS = ['PID reuse, host reboot, disk-full and power-loss durability',
    'full MCP dispatcher restart and response-loss/uncertain-control fault families',
    'simultaneous resume races and escaped/unregistered descendant containment',
    'autonomous workflow-owner replacement during teardown',
    'worktree creation/removal and branch mutation excluded by current authorization',
    'phase layout2/service/acquisition/fairness/cleanup and deep resource borrowing/repair',
    'large history/retention/performance, cross-date statistics, native cost deduplication',
    'all workflow branching/retry/on-demand/goal/context-reference families',
    'stock/real backend, managed runtime closure, Linux/Windows/Rust/UI/CI qualification',
    'S03 Proposed/Undecided scope and prior receipt/aggregate acceptance']


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--invocation-id', required=True)
    parser.add_argument('--repo', required=True, type=Path)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--python', required=True, type=Path)
    parser.add_argument('--observer-preflight', required=True, type=Path)
    parser.add_argument('--observer-preflight-sha256', required=True)
    parser.add_argument('--case', action='append', choices=tuple(SCENARIOS))
    args = parser.parse_args(argv)
    require(re.fullmatch('[0-9a-f]{32}', args.invocation_id) is not None, 'invalid caller invocation identity')
    gate = read_regular(args.observer_preflight)
    require(bool(gate) and sha(gate) == args.observer_preflight_sha256, 'external observer preflight byte identity mismatch')
    root = Path('/private/tmp') / ('h9-' + args.invocation_id[:14])
    root.mkdir(mode=0o700, exist_ok=False)
    context = {'invocation_id': args.invocation_id, 'source_commit': args.commit,
        'observer_preflight_sha256': sha(gate), 'framework_sha256': framework_hash(),
        'requested_cases': args.case or list(SCENARIOS)}
    (root / 'context.json').write_text(json.dumps(context, indent=2, sort_keys=True))
    (root / 'observer-preflight.bin').write_bytes(gate)
    report = {**context, 'format': 'harflow.conformance.s09', 'v': 1, 'cases': [], 'passed': False,
        'profile': 'controlled_source_fixture_injection', 'stock_backend_qualification': False,
        'C0_MC_04_accepted': False, 'gaps': GAPS, 'observer_gate_interpretation':
        'Caller supplied external preflight; runner binds bytes and does not self-attest test success'}
    for index, name in enumerate(context['requested_cases'], 1):
        entry = {'name': name, 'output': str(root / ('case%02d' % index)), 'passed': False}
        case = None
        spec = {'needs_input_question': 'S09 exact caller question Ω', 'answer_token': 'S09_AUTHORED_ANSWER',
                'question_node_id': 'start'} if name == 'workflow-input' else {}
        try:
            with SourceProfile(args.repo, args.commit, entry['output'], args.python, tasks_enabled=True, case_spec=spec) as profile:
                case = Case(profile)
                case.init_git()
                entry['identity'] = profile.identity
                entry['evidence'] = SCENARIOS[name](case)
            entry['passed'] = True
        except Exception as exc:
            entry['failure'] = {'type': type(exc).__name__, 'message': str(exc)[:2000], 'no_replay': True}
        finally:
            if case is not None:
                entry.update(operations=case.operations, accepted=case.accepted, launches=case.launches,
                    workflow_runs=case.workflow_runs, observer_records=case.p.observer.records,
                    git_init=getattr(case, 'git', None))
            report['cases'].append(entry)
            (root / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True))
            print(json.dumps({'case': name, 'passed': entry['passed'],
                              **({'failure': entry['failure']} if 'failure' in entry else {})}), flush=True)
        if not entry['passed']:
            report['not_run'] = context['requested_cases'][index:]
            break
    require(framework_hash() == context['framework_sha256'], 'framework changed during suite')
    report['passed'] = all(entry['passed'] for entry in report['cases'])
    (root / 'report.json').write_text(json.dumps(report, indent=2, sort_keys=True))
    print(json.dumps({'report': str(root / 'report.json'), 'passed': report['passed']}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
