"""Run the lifecycle cases against a pinned target.

    python -m conformance.lifecycle --engine-bin /abs/engine --engine-sha256 DIGEST --output NEW_DIR
    python -m conformance.lifecycle --source-repo REPO --commit SHA --python PY --output NEW_DIR \\
        [--seam python-injection]

Exit status is zero only when every selected case passed and none was blocked. A case needing an agent on a
seam without one is `blocked`, which is neither a pass nor acceptance. Runs on a source launcher or through
the Python injection seam are never `acceptance_eligible`.
"""
import argparse
import json
import os
from pathlib import Path
import re
import sys
import traceback
import uuid

from ..e1.harness import source_target as e1_source_target
from ..oracles import ConformanceError, PINNED_MANIFEST_SHA256, strict_json, read_regular, verify_manifest
from ..runner import isolated_environment, publish_report
from .cases import CASES, Lifecycle
from .seams import NoSeam, PythonInjectionSeam, SeamRequired
from .target import Engine, binary_target, source_target


def run_case(name, function, args, output, target, snapshot_command):
    folder = Path(output) / 'cases' / name
    folder.mkdir(parents=True)
    entry = {'name': name, 'status': 'failed', 'output': str(folder)}
    seam = None
    try:
        if args.seam == 'python-injection':
            seam = PythonInjectionSeam(args.source_repo, args.commit, args.python, folder / 'p')
        else:
            env = isolated_environment(folder)
            Path(env['HARFLOW_STATE_ROOT']).parent.mkdir(exist_ok=True)
            seam = NoSeam(Engine(target, env, folder / 'workspace', folder / 'ops'), folder / 'workspace')
        entry['evidence'] = function(Lifecycle(seam))
        entry['status'] = 'passed'
    except SeamRequired as exc:
        entry.update(status='blocked', reason=str(exc))
    except Exception as exc:
        entry['failure'] = {'type': type(exc).__name__, 'message': str(exc)[:1500], 'trace': traceback.format_exc(limit=3)}
    finally:
        if seam is not None:
            entry['operations'] = [{'argv': op['argv'], 'exit_code': op['exit_code']} for op in seam.operations]
            if not seam.close() and entry['status'] == 'passed':
                entry.update(status='failed', failure={'type': 'ConformanceError', 'message': 'participant cleanup unproved'})
    return entry


def assess(entries, selected):
    failed = [e['name'] for e in entries if e['status'] == 'failed']
    blocked = [e['name'] for e in entries if e['status'] == 'blocked']
    return {'ok': bool(entries) and not failed, 'failed': failed, 'blocked': blocked,
            'complete': not selected and len(entries) == len(CASES) and not failed and not blocked}


def run(args):
    output = Path(args.output).absolute()
    output.mkdir(exist_ok=False)
    invocation = args.invocation_id or uuid.uuid4().hex
    if not re.fullmatch('[a-f0-9]{32}', invocation):
        raise ConformanceError('invalid invocation id')
    verify_manifest(args.manifest, args.manifest_sha256)
    snapshot_command = None
    if args.engine_bin:
        target = binary_target(args.engine_bin, args.engine_sha256)
    elif args.seam == 'python-injection':
        target = None
    else:
        command, identity, check, _ = e1_source_target(args.source_repo, args.commit, args.python, output)
        target = source_target(command, identity, check)
    report = {'format': 'harflow.conformance.lifecycle-report', 'v': 1, 'invocation_id': invocation, 'seam': args.seam,
              'manifest_sha256': args.manifest_sha256, 'target': target.identity if target else {'kind': 'python-injection-profile', 'release_binary': False},
              'cases': []}
    names = args.case or list(CASES)
    for name in names:
        entry = run_case(name, CASES[name], args, output, target, snapshot_command)
        report['cases'].append(entry)
        print(f'{entry["status"].upper()} {name}' + (f': {entry["failure"]["message"][:200]}' if entry['status'] == 'failed' else ''), file=sys.stderr)
        if entry['status'] == 'failed':
            break  # fail closed at the first defect, never replay
    summary = assess(report['cases'], args.case)
    seam_eligible = args.seam == 'none'
    report.update(summary, acceptance_eligible=bool(summary['complete'] and seam_eligible and report['target'].get('release_binary')))
    publish_report(output / 'report.json', report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--engine-bin', default=os.environ.get('HARFLOW_ENGINE_BIN'))
    parser.add_argument('--engine-sha256')
    parser.add_argument('--source-repo')
    parser.add_argument('--commit')
    parser.add_argument('--python')
    parser.add_argument('--seam', choices=('none', 'python-injection'), default='none')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--manifest-sha256', default=PINNED_MANIFEST_SHA256)
    parser.add_argument('--output', required=True)
    parser.add_argument('--invocation-id')
    parser.add_argument('--case', action='append', choices=tuple(CASES))
    args = parser.parse_args(argv)
    binary = bool(args.engine_bin)
    if binary and (args.source_repo or args.commit or args.python or not args.engine_sha256 or args.seam != 'none'):
        parser.error('a pinned binary needs --engine-sha256, no source options and --seam none')
    if not binary and not all((args.source_repo, args.commit, args.python)) or args.engine_sha256 and not binary:
        parser.error('source selection needs --source-repo, a full --commit and --python')
    report = run(args)
    print(json.dumps({k: report[k] for k in ('ok', 'complete', 'acceptance_eligible', 'failed', 'blocked', 'seam')} |
                     {'report': str(Path(args.output).absolute() / 'report.json')}))
    return 0 if report['ok'] and report['complete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
