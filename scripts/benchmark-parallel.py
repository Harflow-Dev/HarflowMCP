#!/usr/bin/env python3
"""Synthetic parallel-group fixtures and append bursts; never dispatches agents."""
from __future__ import annotations
import argparse
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / 'src'))
MARKER = 'parallel-benchmark-fixture.json'
GROUP = 'Synthetic parallel benchmark'


def monitor_module():
    spec = importlib.util.spec_from_file_location('monitor_benchmark', REPO / 'scripts/benchmark-monitor.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def event(task_id, seq, kind, **fields):
    timestamp = (datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(seconds=seq)).isoformat()
    return dict(v=1, seq=seq, observed_at=timestamp, source_ts=None, raw_offset=None,
                task_id=task_id, kind=kind, **fields)


def fixture(home: Path, conversations=8, chain=1, activity='sparse', workflow=False):
    from polybridge import store
    if conversations not in (8, 64, 256) or chain not in (1, 16) or activity not in ('sparse', 'long'):
        raise ValueError('Unsupported fixture shape')
    if home.is_symlink() or (home.exists() and any(home.iterdir())):
        raise ValueError('Fixture HOME must be empty')
    # Reuse only isolated wrappers and fixture marker; replace synthetic inventory entirely.
    home.mkdir(parents=True, exist_ok=True)
    monitor_module().fixture(home, 'small')
    import shutil
    shutil.rmtree(home / '.polybridge')
    directory = home / '.polybridge' / 'tasks'
    directory.mkdir(parents=True)
    for conversation in range(conversations):
        for turn in range(chain):
            task_id = f'parallel-{conversation:04}-{turn:02}'
            prior = f'parallel-{conversation:04}-{turn-1:02}' if turn else None
            timestamp = (datetime(2026, 10, 1, tzinfo=timezone.utc) + timedelta(seconds=conversation*chain+turn)).isoformat()
            record = store.TaskRecord(task_id, 'codex', f'parallel-session-{conversation:04}', '/benchmark', timestamp,
                status='completed', exit_code=0, group=GROUP, parent_task_id=prior,
                title=f'Synthetic conversation {conversation:04}', prompt='Synthetic benchmark input')
            (directory / f'{task_id}.meta.json').write_text(json.dumps(asdict(record), sort_keys=True))
            rows = [event(task_id, 0, 'task_started', backend='codex'), event(task_id, 1, 'user_message', text='Synthetic benchmark input')]
            for index in range(2 if activity == 'sparse' else 256):
                call_id = f'call-{index:04}'
                rows.append(event(task_id, len(rows), 'tool_call', call_id=call_id, tool='read', category='read', input={'path': 'synthetic.txt'}))
                rows.append(event(task_id, len(rows), 'tool_result', call_id=call_id, ok=True, output='Synthetic output'))
            rows.append(event(task_id, len(rows), 'assistant_text', text='Synthetic benchmark answer'))
            rows.append(event(task_id, len(rows), 'task_finished', status='completed', exit_code=0))
            (directory / f'{task_id}.events.jsonl').write_text(''.join(json.dumps(row, sort_keys=True)+'\n' for row in rows))
            raw = [{'type':'thread.started','thread_id':record.session_id}, {'type':'item.completed','item':{'id':'answer','type':'agent_message','text':'Synthetic benchmark answer'}}, {'type':'turn.completed','usage':{'input_tokens':0,'output_tokens':0,'cached_input_tokens':0}}]
            (directory / f'{task_id}.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in raw))
    if workflow:
        from polybridge import workflows
        storage = workflows.WorkflowStore(home / '.polybridge')
        definition = workflows.validate_definition({'name': 'Synthetic parallel workflow', 'nodes': [
            {'id': 'start', 'type': 'start'}, {'id': 'work', 'type': 'agent', 'name': 'Synthetic parallel tasks', 'agent': {'backend': 'codex'}},
            {'id': 'end', 'type': 'end'}], 'connections': [
            {'id': 'a', 'source': 'start', 'target': 'work'}, {'id': 'b', 'source': 'work', 'target': 'end'}],
            'orchestrator': {'backend': 'codex'}})
        definition.update(workflow_id='parallel-workflow', revision=1)
        (storage.definitions / 'parallel-workflow.json').write_text(json.dumps(definition, sort_keys=True))
        task_ids = [f'parallel-{conversation:04}-{turn:02}' for conversation in range(conversations) for turn in range(chain)]
        associations = [{'task_id': task_id, 'backend': 'codex', 'status': 'completed', 'summary': 'Synthetic benchmark answer'} for task_id in task_ids]
        run = dict(workflow_run_id='parallel-run', kind='workflow', name='Synthetic parallel workflow', definition=definition,
            revision=1, prompt='Synthetic benchmark input', repo_path='/benchmark', freedom='read_only', status='completed',
            created_at=1790812800.0, updated_at=1790812800.0, sequence=1, decisions=[], sessions={}, pending=[],
            tasks=associations, joins={}, instructions='', supervisor_pid=None, summary='Synthetic parallel benchmark',
            activations=[dict(id='parallel-execution', node_id='work', role='node', status='completed', created_at=1790812800.0,
                tasks=associations, summary='Synthetic parallel benchmark', node_result={'status': 'succeeded', 'result': {}, 'evidence': []})])
        (storage.runs / 'parallel-run.json').write_text(json.dumps(run, sort_keys=True))
    manifest = dict(version=1, conversations=conversations, chain=chain, activity=activity, tasks=conversations*chain, group=GROUP, workflow=workflow)
    (home / MARKER).write_text(json.dumps(manifest, sort_keys=True))
    (home / 'monitor-benchmark-fixture.json').write_text(json.dumps(dict(version=1, size='parallel', definitions=int(workflow), runs=int(workflow), tasks=manifest['tasks'])))
    return manifest


def append_burst(home: Path, count=32):
    """Append synthetic activity to every current conversation checkpoint, preserving sequence."""
    if count < 1 or count > 1024:
        raise ValueError('Burst count must be 1..1024')
    manifest = json.loads((home / MARKER).read_text())
    if home.is_symlink() or not isinstance(manifest, dict) or manifest.get('version') != 1 or manifest.get('conversations') not in (8, 64, 256) or manifest.get('chain') not in (1, 16):
        raise ValueError('Unsupported fixture marker')
    targets = []
    for conversation in range(manifest['conversations']):
        task_id = f"parallel-{conversation:04}-{manifest['chain']-1:02}"
        path = home / '.polybridge' / 'tasks' / f'{task_id}.events.jsonl'
        if path.is_symlink() or path.resolve().parent != (home / '.polybridge' / 'tasks').resolve() or (home / '.polybridge').is_symlink() or (home / '.polybridge' / 'tasks').is_symlink():
            raise ValueError('Fixture event path must be local')
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        if not rows or any(not isinstance(row, dict) or row.get('task_id') != task_id or row.get('v') != 1 or row.get('seq') != index for index, row in enumerate(rows)):
            raise ValueError('Fixture event identity mismatch')
        targets.append((task_id, path, rows[-1]['seq']+1))
    for task_id, path, seq in targets:
        with path.open('a') as output:
            for index in range(count):
                output.write(json.dumps(event(task_id, seq+index, 'notice', text='Synthetic burst activity'))+'\n')
    return dict(tasks_appended=len(targets), events_appended=len(targets)*count)


def protocol():
    return {'version':1, 'shapes': [{'conversations':n,'chain':c,'activity':a} for n in (8,64,256) for c in (1,16) for a in ('sparse','long')],
        'repeats':10, 'scenarios':['horizontal scrolling','rapid reversals','column actions','Show all','return to visited columns','unchanged polling','burst on all current checkpoints'],
        'record':['first useful content ms','complete preparation ms','snapshot/build/apply p50 and p95 ms','main-thread stalls','peak RSS bytes','CLI process count','leased task count'],
        'notes':['Use isolated copied app via profile-monitor.py; never install.', 'Collect baseline and optimized runs under comparable host load.', 'Record UI observation overhead separately; readiness timestamps are applied state, not pixels.', 'Burst events are synthetic post-completion activity for tailing stress, not a real agent lifecycle.']}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture-home', type=Path)
    parser.add_argument('--conversations', type=int, choices=(8,64,256), default=8)
    parser.add_argument('--chain', type=int, choices=(1,16), default=1)
    parser.add_argument('--activity', choices=('sparse','long'), default='sparse')
    parser.add_argument('--append-burst', type=int)
    parser.add_argument('--workflow', action='store_true', help='Associate all checkpoints with a synthetic workflow execution for embedded parallel columns')
    args=parser.parse_args()
    if args.append_burst is not None:
        if args.fixture_home is None: parser.error('--append-burst requires --fixture-home')
        result=append_burst(args.fixture_home, args.append_burst)
    elif args.fixture_home:
        result=fixture(args.fixture_home,args.conversations,args.chain,args.activity,args.workflow)
    else: result=protocol()
    print(json.dumps(result,indent=2))

if __name__ == '__main__': main()
