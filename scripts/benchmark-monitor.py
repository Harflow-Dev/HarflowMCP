#!/usr/bin/env python3
"""Isolated CLI baseline. No agents, installed tools, or production history are used."""
from __future__ import annotations
import argparse
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import platform
import resource
import shlex
import subprocess
import sys
import tempfile
import time

SIZES = {'small': (12, 24, 32), 'large': (240, 400, 1200)}
REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / 'src'))


def fixture(home: Path, size: str) -> dict:
    from polybridge import store, workflows
    root = home / '.polybridge'
    if root.exists():
        raise ValueError('Fixture destination must be empty')
    definitions, runs, tasks = SIZES[size]
    storage = workflows.WorkflowStore(root)
    definition = workflows.validate_definition({'name': 'bench', 'nodes': [
        {'id': 'start', 'type': 'start'}, {'id': 'work', 'type': 'agent', 'agent': {'backend': 'codex'}},
        {'id': 'end', 'type': 'end'}], 'connections': [
        {'id': 'a', 'source': 'start', 'target': 'work'}, {'id': 'b', 'source': 'work', 'target': 'end'}],
        'orchestrator': {'backend': 'codex'}})
    for i in range(definitions):
        value = definition | {'name': f'bench-{i:04}', 'revision': 1, 'workflow_id': f'bench-{i:04}'}
        (storage.definitions / f'bench-{i:04}.json').write_text(json.dumps(value))
    for i in range(runs):
        rid = f'run-{i:04}'
        run = {'workflow_run_id': rid, 'kind': 'workflow', 'name': 'bench', 'definition': definition,
               'revision': 1, 'prompt': 'Synthetic benchmark input', 'repo_path': '/benchmark',
               'freedom': 'read_only', 'status': 'needs_input' if i % 8 == 0 else 'completed',
               'created_at': 1790812800.0 + i, 'updated_at': 1790812800.0 + i,
               'sequence': 1, 'activations': [], 'decisions': [], 'sessions': {}, 'pending': [],
               'tasks': [], 'joins': {}, 'instructions': '', 'supervisor_pid': None,
               'summary': 'Synthetic summary. ' * (20000 if i == 0 else 20)}
        if i == 0:
            run['activations'] = [{'id': f'execution-{n:04}', 'node_id': 'work', 'role': 'node', 'status': 'completed', 'created_at': 1790812800.0 + n, 'tasks': [{'task_id': f'task-{n:04}', 'backend': 'codex', 'status': 'completed', 'summary': 'Synthetic worker summary'}], 'summary': 'Synthetic worker summary', 'node_result': {'status': 'succeeded', 'result': {}, 'evidence': []}} for n in range(8 if size == 'small' else 200)]
        if i % 2 == 0:
            run['activations'].append({'id': 'child-call', 'node_id': 'work', 'role': 'node', 'status': 'completed', 'tasks': [], 'invocation': {'child_workflow_run_id': f'run-{i+1:04}', 'workflow_id': 'bench-0000', 'stage': 'settled'}})
        if i % 2:
            run['parent_link'] = {'workflow_run_id': f'run-{i-1:04}', 'root_workflow_run_id': f'run-{i-1:04}', 'execution_id': 'child-call'}
        (storage.runs / f'{rid}.json').write_text(json.dumps(run))
    directory = root / 'tasks'
    directory.mkdir()
    for i in range(tasks):
        value = store.TaskRecord(f'task-{i:04}', 'codex', 'synthetic-session', '/benchmark',
                                 '2026-10-01T00:00:00+00:00', status='completed', exit_code=0,
                                 title=f'Synthetic task {i}')
        (directory / f'{value.task_id}.meta.json').write_text(json.dumps(asdict(value)))
        events = [{'type': 'thread.started', 'thread_id': 'synthetic-session'}, {'type': 'item.completed', 'item': {'id': 'answer', 'type': 'agent_message', 'text': f'Synthetic task summary {i}'}}, {'type': 'turn.completed', 'usage': {'input_tokens': 0, 'cached_input_tokens': 0, 'output_tokens': 0}}]
        (directory / f'{value.task_id}.jsonl').write_text(''.join(json.dumps(event) + '\n' for event in events))
    # Persistent malformed history exercises the blocked projection without permission changes.
    blocked = home / 'blocked-fixture' / '.polybridge' / 'tasks'
    blocked.mkdir(parents=True)
    (blocked / 'blocked.meta.json').write_text('{')
    manifest = {'version': 1, 'size': size, 'definitions': definitions, 'runs': runs, 'tasks': tasks,
            'blocked_records': 1, 'nested_runs': runs // 2, 'snapshot_summary_chars': 380000}
    (home / 'monitor-benchmark-fixture.json').write_text(json.dumps(manifest))
    binaries = home / '.local' / 'bin'
    binaries.mkdir(parents=True)
    wrapper = binaries / 'polybridge-ctl'
    wrapper.write_text('#!/bin/sh\ncase "$1" in backends|workflow-validate|workflow-get|workflow-list-page|workflow-list|task-list-page|list|status|workflow-status|workflow-detail|workflow-list-runs) ;; *) exit 2 ;; esac\nexport HOME=' + shlex.quote(str(home.resolve())) + '\nexec ' + shlex.quote(str(REPO / '.venv/bin/python')) + ' ' + shlex.quote(str(Path(__file__).resolve())) + ' --worker "$@"\n')
    wrapper.chmod(0o755)
    setup = binaries / 'polybridge-setup'
    setup.write_text("#!/bin/sh\n[ \"$*\" = \"--status --json\" ] || exit 2\necho '{\"v\":1,\"server_path\":null,\"clients\":[]}'\n")
    setup.chmod(0o755)
    return manifest


def percentile(values, percentile):
    return sorted(values)[max(0, math.ceil(len(values) * percentile) - 1)]


def worker(args):
    allowed = {'workflow-validate', 'workflow-get', 'workflow-list-page', 'workflow-list', 'task-list-page', 'list', 'status', 'backends', 'workflow-status', 'workflow-detail', 'workflow-list-runs'}
    if not args or args[0] not in allowed:
        print('Benchmark fixture permits read commands only', file=sys.stderr)
        return 2
    from polybridge import catalog, ctl, lineage
    # Explicit synthetic human ancestry avoids host process-table permission variability.
    lineage._default_process_table = lambda: {os.getpid(): 1, 1: 0}
    original = catalog.Catalog.bootstrap
    durations = []
    def measured(self, *a, **kw):
        start = time.perf_counter()
        try:
            return original(self, *a, **kw)
        finally:
            durations.append((time.perf_counter() - start) * 1000)
    catalog.Catalog.bootstrap = measured
    start = time.perf_counter()
    result = ctl.main(args)
    print(json.dumps({'benchmark_metrics': {'ctl_ms': (time.perf_counter()-start)*1000,
          'catalog_bootstrap_ms': sum(durations), 'catalog_bootstrap_calls': len(durations),
          'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss *
                            (1 if sys.platform == 'darwin' else 1024)}}), file=sys.stderr)
    return result


def invoke(home, args):
    env = {k: v for k, v in os.environ.items() if not k.startswith('PB_')}
    env['HOME'] = str(home)
    start = time.perf_counter()
    completed = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--worker', *args],
                               env=env, capture_output=True, check=True)
    elapsed = (time.perf_counter()-start)*1000
    decode = time.perf_counter()
    response = json.loads(completed.stdout)
    decode_ms = (time.perf_counter()-decode)*1000
    metrics = next(json.loads(line)['benchmark_metrics'] for line in completed.stderr.decode().splitlines()
                   if line.startswith('{"benchmark_metrics"'))
    return response.get('result', response), metrics | {'wall_ms': elapsed, 'decode_ms': decode_ms,
        'response_bytes': len(completed.stdout), 'stderr_bytes': len(completed.stderr), 'cli_processes': 1}


def scenario(home, command, *, paginate=False, prepare=False):
    samples = []
    cursor = None
    first_useful = None
    start = time.perf_counter()
    for _ in range(1000):
        response, sample = invoke(home, command + (['--cursor', cursor] if cursor else []))
        samples.append(sample)
        if first_useful is None and not response.get('bootstrap_pending') and response.get('catalog_state', {}).get('status') != 'blocked':
            first_useful = (time.perf_counter()-start)*1000
        if prepare and response.get('bootstrap_pending') and response.get('catalog_state', {}).get('status') != 'blocked':
            continue
        cursor = response.get('next_cursor') if paginate else None
        if not cursor:
            break
    else:
        raise RuntimeError('Benchmark failed to settle within 1000 reads')
    elapsed = (time.perf_counter()-start)*1000
    blocked = response.get('catalog_state', {}).get('status') == 'blocked'
    return {'terminal_catalog_state': response.get('catalog_state', {}).get('status'), 'first_ready_response_cli_ms': first_useful,
            'terminal_cli_ms': elapsed, 'complete_cli_ms': None if blocked else elapsed,
            'samples': samples}


def benchmark(size, repeats):
    results = []
    manifest = None
    for _ in range(repeats):
        with tempfile.TemporaryDirectory(prefix='pb-monitor-benchmark-') as temporary:
            home = Path(temporary)
            manifest = fixture(home, size)
            for name, command, paginate, prepare in [
                ('cold_launch', ['workflow-list-page', '--json'], False, True),
                ('cold_tasks', ['task-list-page', '--json'], False, True),
                ('task_summary', ['status', 'task-0000', '--json'], False, False),
                ('saved_workflow_open', ['workflow-get', 'bench-0000', '--json'], False, False),
                ('cold_workflow_open', ['workflow-status', 'run-0000', '--monitor-view', '--snapshot', '--json'], True, False),
                ('warm_reopen', ['workflow-status', 'run-0000', '--monitor-view', '--json'], False, False),
                ('workflow_switch', ['workflow-status', 'run-0001', '--monitor-view', '--snapshot', '--json'], True, False),
                ('unchanged_poll', ['workflow-status', 'run-0000', '--monitor-view', '--json'], False, False),
                ('history_pagination', ['workflow-list-page', '--json'], True, True)]:
                record = scenario(home, command, paginate=paginate, prepare=prepare)
                if name in {'cold_workflow_open', 'workflow_switch'}:
                    index = scenario(home, ['workflow-detail', command[1], '--monitor-view', '--view', 'execution_index', '--json'], paginate=True)
                    record['samples'].extend(index['samples'])
                    record['complete_cli_ms'] += index['complete_cli_ms']
                    record['terminal_cli_ms'] += index['terminal_cli_ms']
                results.append({'scenario': name, **record})
            results.append({'scenario': 'blocked_history', **scenario(home / 'blocked-fixture', ['task-list-page', '--json'])})
    summary = {}
    for name in dict.fromkeys(r['scenario'] for r in results):
        records = [r for r in results if r['scenario'] == name]
        values = [r['terminal_cli_ms'] for r in records]
        summary[name] = {'n': len(values), 'percentile_method': 'nearest rank',
                         'latency_boundary': 'terminal blocked response' if name == 'blocked_history' else 'complete CLI sequence',
                         'p50_ms': percentile(values, .5), 'p95_ms': percentile(values, .95),
                         'cli_processes_per_repeat': [sum(s['cli_processes'] for s in r['samples']) for r in records],
                         'transport_bytes_per_repeat': [sum(s['response_bytes'] + s['stderr_bytes'] for s in r['samples']) for r in records]}
    return {'version': 1, 'fixture': manifest, 'repeats': repeats, 'python': sys.version,
            'platform': platform.platform(), 'summary': summary, 'raw': results,
            'limitations': 'CLI proxy only; synthetic human ancestry; warm reopening assumes existing digest cache; warm OS caches; no UI readiness, reconciliation, main-thread stalls or app memory measured. RSS is the CLI child high-water mark. Cold means fresh catalog, not dropped filesystem caches.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--size', choices=SIZES, default='small')
    parser.add_argument('--repeats', type=int, default=5)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--fixture-home', type=Path, help='Create a retained fixture in an empty HOME and exit')
    args = parser.parse_args()
    if args.fixture_home:
        args.fixture_home.mkdir(parents=True, exist_ok=True)
        print(json.dumps(fixture(args.fixture_home, args.size), indent=2))
        return
    if args.repeats < 1:
        parser.error('--repeats must be positive')
    result = json.dumps(benchmark(args.size, args.repeats), indent=2)
    if args.output:
        args.output.write_text(result + '\n')
    else:
        print(result)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--worker':
        raise SystemExit(worker(sys.argv[2:]))
    main()
