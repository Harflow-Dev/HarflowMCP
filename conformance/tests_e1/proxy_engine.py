"""Test double: forwards to the real ctl but corrupts one named aspect of its answer.

    proxy_engine.py MODE CTL_ARGV... -- ARGS...

Used only by the E1 infrastructure self-checks to show that the scenarios fail when an engine
deviates; it is not an engine and carries no contract knowledge of its own. Each time a mutation is
actually applied it appends a line to `proxy-applied.log` next to the state root, so a self-check can
prove the deviation really happened.
"""
import glob
import json
import os
import subprocess
import sys

mode = sys.argv[1]
split = sys.argv.index('--')
real, args = sys.argv[2:split], sys.argv[split + 1:]
environment = dict(os.environ)
if mode == 'strip-markers':
    for key in ('HARFLOW_TASK_ID', 'PB_TASK_ID', 'HARFLOW_DEPTH', 'PB_DEPTH'):
        environment.pop(key, None)
done = subprocess.run(real + args, capture_output=True, env=environment)
out = done.stdout
root = os.environ.get('HARFLOW_STATE_ROOT', '')
LOG = os.path.join(os.path.dirname(root), 'proxy-applied.log') if root else None


def applied(note):
    if LOG:
        with open(LOG, 'a') as handle:
            handle.write(f'{mode} {args[0]} {note}\n')


if mode == 'strip-markers' and any(k in os.environ for k in ('HARFLOW_TASK_ID', 'PB_TASK_ID', 'HARFLOW_DEPTH', 'PB_DEPTH')) and args[0] != 'version-info':
    applied('markers stripped')
if mode == 'mutate-state' and args[0] != 'version-info':
    for path in glob.glob(os.path.join(root, 'tasks', '*.events.jsonl'))[:1]:
        with open(path, 'ab') as handle:
            handle.write(b'\n')
        applied('state rewritten')
if mode == 'empty' and args[0] != 'version-info':
    applied('empty answer')
    sys.stdout.write('{}\n')
    sys.exit(0)
if mode not in ('passthrough', 'strip-markers', 'mutate-state') and out.strip() and args[0] != 'version-info':
    try:
        doc = json.loads(out)
    except ValueError:
        doc = None
    if isinstance(doc, dict):
        task = doc.get('task')
        error = doc.get('error') or {}
        result = doc.get('result')
        if mode == 'drop-resolved-from' and isinstance(task, dict) and 'resolved_from' in task:
            task.pop('resolved_from')
            applied('resolved_from dropped')
        if mode == 'wrong-status' and isinstance(task, dict):
            task['status'] = 'running'
            applied('status changed')
        if mode == 'no-authority' and error.get('code') == 'agent_caller':
            doc = {k: v for k, v in doc.items() if k != 'error'}
            doc['result'] = {'events': [], 'task_id': 'x'}
            done.returncode = 0
            applied('agent_caller refusal removed')
        if mode == 'open-undecidable' and error.get('code') in ('caller_undecidable', 'workflow_read_refused', 'unauthorized'):
            doc = {k: v for k, v in doc.items() if k != 'error'}
            doc['result'] = {'tasks': [], 'events': []}
            done.returncode = 0
            applied('undecidable refusal removed')
        if mode == 'unbound-cursor' and error.get('code') in ('invalid_params', 'invalid_cursor', 'snapshot_expired', 'event_cursor_expired') \
                and ('cursor' in error.get('message', '').lower() or error.get('code') != 'invalid_params'):
            doc = {k: v for k, v in doc.items() if k != 'error'}
            doc['result'] = {'items': [], 'events': [], 'has_more': False}
            done.returncode = 0
            applied('cursor refusal removed')
        if mode == 'mutate-events' and isinstance(result, dict) and result.get('events'):
            result['events'] = result['events'][1:]
            applied('event dropped')
        if mode == 'loop' and isinstance(result, dict) and ('has_newer' in result or 'cycle_complete' in result or 'has_more' in result):
            for key, value in (('has_newer', True), ('cycle_complete', False), ('has_more', True)):
                if key in result:
                    result[key] = value
            for key in ('newer_cursor', 'next_cursor'):
                if key in result:
                    result[key] = result[key] or 'c'
            applied('paging made nonterminating')
        out = (json.dumps(doc) + '\n').encode()
sys.stdout.write(out.decode())
sys.stderr.write(done.stderr.decode())
sys.exit(done.returncode)
