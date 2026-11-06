"""Test double: forwards to the real ctl but corrupts one named aspect of its answer.

    proxy_engine.py MODE CTL_ARGV... -- ARGS...

Used only by the E1 infrastructure self-checks to show that the scenarios fail when an engine
deviates; it is not an engine and carries no contract knowledge of its own.
"""
import glob
import json
import os
import subprocess
import sys

mode = sys.argv[1]
split = sys.argv.index('--')
real, args = sys.argv[2:split], sys.argv[split + 1:]
done = subprocess.run(real + args, capture_output=True)
out = done.stdout
if mode == 'mutate-state' and args[0] != 'version-info':
    for path in glob.glob(os.path.join(os.environ['HARFLOW_STATE_ROOT'], 'tasks', '*.events.jsonl'))[:1]:
        with open(path, 'ab') as handle:
            handle.write(b'\n')
if mode == 'empty' and args[0] != 'version-info':
    sys.stdout.write('{}\n')
    sys.exit(0)
if mode != 'passthrough' and out.strip() and args[0] != 'version-info':
    try:
        doc = json.loads(out)
    except ValueError:
        doc = None
    if isinstance(doc, dict):
        task = doc.get('task')
        if mode == 'drop-resolved-from' and isinstance(task, dict):
            task.pop('resolved_from', None)
        if mode == 'wrong-status' and isinstance(task, dict):
            task['status'] = 'running'
        if mode == 'no-authority' and (doc.get('error') or {}).get('code') == 'agent_caller':
            doc = {k: v for k, v in doc.items() if k != 'error'}
            doc['result'] = {'events': [], 'task_id': 'x'}
            done.returncode = 0
        if mode == 'mutate-events' and isinstance(doc.get('result'), dict) and 'events' in doc['result']:
            doc['result']['events'] = doc['result']['events'][1:]
        out = (json.dumps(doc) + '\n').encode()
sys.stdout.write(out.decode())
sys.stderr.write(done.stderr.decode())
sys.exit(done.returncode)
