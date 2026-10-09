#!/usr/bin/env python3
"""Repeat existing synthetic CLI scenarios against an explicit source checkout.

Run before and after with the same interpreter. No installed executable is used.
Startup is parent-observed wall time; import is child-observed ctl import time.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

SCRIPT = Path(__file__).resolve()


def worker(source: Path, commands: list[str]) -> int:
    allowed = {'workflow-validate', 'workflow-get', 'workflow-list-page', 'workflow-list',
               'task-list-page', 'list', 'status', 'backends', 'workflow-status',
               'workflow-detail', 'workflow-list-runs'}
    if not commands or commands[0] not in allowed:
        print('Benchmark fixture permits read commands only', file=sys.stderr)
        return 2
    sys.path.insert(0, str(source / 'src'))
    entry = time.perf_counter()
    launch_marker = os.environ.get('PB_BENCH_LAUNCH_CLOCK')
    launch = float(launch_marker) if launch_marker is not None else None
    started = time.perf_counter()
    from polybridge import ctl
    imported = (time.perf_counter() - started) * 1000
    from polybridge import catalog, lineage
    lineage._default_process_table = lambda: {os.getpid(): 1, 1: 0}
    original = catalog.Catalog.bootstrap
    durations = []

    def measured(self, *args, **kwargs):
        started = time.perf_counter()
        try:
            return original(self, *args, **kwargs)
        finally:
            durations.append((time.perf_counter() - started) * 1000)

    catalog.Catalog.bootstrap = measured
    started = time.perf_counter()
    code = ctl.main(commands)
    print(json.dumps({'benchmark_metrics': {
        'ctl_ms': (time.perf_counter() - started) * 1000,
        'ctl_import_ms': imported,
        'interpreter_script_setup_ms': (entry - launch) * 1000 if launch is not None else None,
        'catalog_bootstrap_ms': sum(durations),
        'catalog_bootstrap_calls': len(durations),
        'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss *
                          (1 if sys.platform == 'darwin' else 1024)}}), file=sys.stderr)
    return code


def load_baseline(source):
    # Reuse exactly the established fixtures and command sequences, not a lighter
    # replacement dataset. Their wrappers are never invoked by this runner.
    sys.path.insert(0, str(source / 'src'))
    spec = importlib.util.spec_from_file_location('monitor_benchmark', source / 'scripts/benchmark-monitor.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--size', choices=['small', 'large'], default='small')
    parser.add_argument('--repeats', type=int, default=10)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--read-timings', choices=['on', 'off'], default='on')
    parser.add_argument('--series-concurrency', choices=['serialized', 'paired-sizes', 'unspecified'], default='unspecified')
    parser.add_argument('--worker', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    source = args.source.resolve()
    if args.worker is not None:
        return worker(source, args.worker)
    if args.repeats < 1:
        parser.error('--repeats must be positive')
    baseline = load_baseline(source)

    def invoke(home, commands):
        env = {k: v for k, v in os.environ.items() if not k.startswith('PB_')}
        env.update(HOME=str(home), PB_WORKFLOW_READ_METRICS='1' if args.read_timings == 'on' else '0')
        started = time.perf_counter()
        env['PB_BENCH_LAUNCH_CLOCK'] = str(started)
        completed = subprocess.run([sys.executable, str(SCRIPT), '--source', str(source), '--worker', *commands],
                                   env=env, capture_output=True, check=True)
        elapsed = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        response = json.loads(completed.stdout)
        decode_ms = (time.perf_counter() - started) * 1000
        stages = {}
        metrics = None
        for line in completed.stderr.decode().splitlines():
            try:
                value = json.loads(line)
            except ValueError:
                continue
            if 'benchmark_metrics' in value:
                metrics = value['benchmark_metrics']
            if value.get('workflow_read_metric_version') == 1:
                stage = value['stage']
                stages[stage] = stages.get(stage, 0) + value['duration_ms']
        if metrics is None:
            raise RuntimeError('Missing worker timing')
        return response.get('result', response), metrics | {
            'wall_ms': elapsed, 'decode_ms': decode_ms,
            'response_bytes': len(completed.stdout), 'stderr_bytes': len(completed.stderr),
            'cli_processes': 1, 'read_stages_ms': stages}

    baseline.invoke = invoke
    result = baseline.benchmark(args.size, args.repeats)
    result['version'] = 2
    for name, summary in result['summary'].items():
        records = [record for record in result['raw'] if record['scenario'] == name]
        stage_names = sorted({stage for record in records for sample in record['samples']
                              for stage in sample['read_stages_ms']})
        summary['read_stages_ms'] = {}
        for stage in stage_names:
            values = [sum(sample['read_stages_ms'].get(stage, 0) for sample in record['samples'])
                      for record in records]
            summary['read_stages_ms'][stage] = {'p50': baseline.percentile(values, .5),
                                              'p95': baseline.percentile(values, .95)}
        summary['response_bytes_per_repeat'] = [sum(sample['response_bytes'] for sample in record['samples'])
                                                for record in records]
        summary['diagnostic_bytes_per_repeat'] = [sum(sample['stderr_bytes'] for sample in record['samples'])
                                                  for record in records]
    result['source_revision'] = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'],
                                                       text=True).strip() if (source / '.git').exists() else 'archive'
    result['observation_concurrency'] = args.series_concurrency
    result['read_timing_instrumentation'] = args.read_timings
    result['startup_boundary'] = 'Parent subprocess wall time includes interpreter startup, imports, command and shutdown; ctl_import_ms excludes interpreter startup. interpreter_script_setup_ms measures parent launch through worker entry, including script imports and argument parsing. Instrumented runs include stderr timing overhead.'
    result['limitations'] += ' Stage attribution available only when the source implements opt-in read timing. Catalog bootstrap is separately wrapped in both revisions. Worker imports catalog/lineage for synthetic authority and catalog timing; this observation overhead is held constant and is not an uninstrumented production CLI startup estimate.'
    encoded = json.dumps(result, indent=2) + '\n'
    if args.output:
        args.output.write_text(encoded)
    else:
        print(encoded, end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
