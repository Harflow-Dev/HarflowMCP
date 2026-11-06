"""Run the lifecycle cases against a pinned target.

    python -m conformance.lifecycle --engine-bin /abs/engine --engine-sha256 DIGEST --product-version X.Y.Z \\
        --release-sha256 DIGEST --release-commit SHA --manifest M --output NEW_DIR
    python -m conformance.lifecycle --source-repo REPO --commit SHA --python PY --product-version X.Y.Z \\
        --manifest M --output NEW_DIR [--seam python-injection]

Vocabulary of the printed summary and report:

- `no_failures`: no selected case failed. It says nothing about blocked cases.
- `all_passed`: every selected case passed and none was blocked.
- `complete`: all six cases ran and passed. A `--case` subset can be `all_passed` but never `complete`.
- `acceptance_eligible`: complete, the seam is eligible, and the pinned binary equals an *independently supplied*
  release digest and commit. Source launchers and the Python injection seam are never eligible.

Linux is unavailable: the observer needs Darwin kqueue and LOCAL_PEERPID. The exit status is zero only when `complete`.
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
from ..oracles import ConformanceError, verify_manifest
from ..runner import isolated_environment, publish_report
from .cases import CASES, Lifecycle
from .seams import NoSeam, PythonInjectionSeam, SeamRequired
from .target import Engine, binary_target, release_match, source_target

OBSERVER_AVAILABLE = sys.platform == 'darwin'


def run_case(name, function, args, output, target):
    folder = Path(output) / 'cases' / name
    folder.mkdir(parents=True)
    entry = {'name': name, 'status': 'failed', 'output': str(folder)}
    seam = None
    try:
        if args.seam == 'python-injection':
            if not OBSERVER_AVAILABLE:
                raise ConformanceError('lifecycle observation is unavailable on this platform (Darwin only)')
            seam = PythonInjectionSeam(args.source_repo, args.commit, args.python, folder / 'p')
        else:
            env = isolated_environment(folder)
            markers = [str(folder), target.command[0] if target.identity.get('pinned_binary') else None]
            seam = NoSeam(Engine(target, env, folder / 'workspace', folder / 'ops'), folder / 'workspace', markers)
        entry['evidence'] = function(Lifecycle(seam))
        entry['status'] = 'passed'
    except SeamRequired as exc:
        entry.update(status='blocked', reason=str(exc))
    except Exception as exc:
        entry['failure'] = {'type': type(exc).__name__, 'message': str(exc)[:1500], 'trace': traceback.format_exc(limit=3)}
    finally:
        if seam is not None:
            entry['seam_acceptance_eligible'] = seam.acceptance_eligible
            entry['extra_env_keys'] = seam.extra_env_keys
            entry['operations'] = [{'argv': op['argv'], 'exit_code': op['exit_code']} for op in seam.operations]
            try:
                cleaned = bool(seam.close())
                error = None
            except Exception as exc:  # cleanup is evidence too: record it, never let it mask the case outcome
                cleaned, error = False, f'{type(exc).__name__}: {exc}'[:300]
            entry['cleanup'] = {'ok': cleaned, **({'error': error} if error else {})}
            if not cleaned and entry['status'] in ('passed', 'blocked'):
                entry.update(status='failed', failure={'type': 'ConformanceError', 'message': 'participant cleanup unproved'})
    return entry


def assess(entries, selected):
    failed = [e['name'] for e in entries if e['status'] == 'failed']
    blocked = [e['name'] for e in entries if e['status'] == 'blocked']
    no_failures = bool(entries) and not failed
    all_passed = no_failures and not blocked
    return {'no_failures': no_failures, 'all_passed': all_passed, 'failed': failed, 'blocked': blocked,
            'subset': bool(selected),
            'complete': all_passed and not selected and len(entries) == len(CASES)}


def run(args):
    output = Path(args.output).absolute()
    output.mkdir(exist_ok=False)
    invocation = args.invocation_id or uuid.uuid4().hex
    if not re.fullmatch('[a-f0-9]{32}', invocation):
        raise ConformanceError('invalid invocation id')
    verify_manifest(args.manifest, args.manifest_sha256)
    if args.engine_bin:
        target = binary_target(args.engine_bin, args.engine_sha256, args.product_version)
    elif args.seam == 'python-injection':
        target = None
    else:
        command, identity, check, _ = e1_source_target(args.source_repo, args.commit, args.python, output)
        target = source_target(command, identity, check, args.product_version)
    identity = target.identity if target else {'kind': 'python-injection-profile', 'pinned_binary': False, 'product_version': args.product_version}
    report = {'format': 'harflow.conformance.lifecycle-report', 'v': 2, 'invocation_id': invocation, 'seam': args.seam,
              'manifest_sha256': args.manifest_sha256, 'target': identity,
              'release_identity': {'sha256': args.release_sha256, 'commit': args.release_commit},
              'platform': {'os': sys.platform, 'observer_available': OBSERVER_AVAILABLE,
                           'linux_lifecycle': 'unavailable'}, 'cases': []}
    for name in args.case or list(CASES):
        entry = run_case(name, CASES[name], args, output, target)
        report['cases'].append(entry)
        print(f'{entry["status"].upper()} {name}' + (f': {entry["failure"]["message"][:200]}' if entry['status'] == 'failed' else ''), file=sys.stderr)
        if entry['status'] == 'failed':
            break  # fail closed at the first defect, never replay
    summary = assess(report['cases'], args.case)
    seam_eligible = bool(report['cases']) and all(e.get('seam_acceptance_eligible') for e in report['cases'])
    report.update(summary, extra_env_keys=sorted({k for e in report['cases'] for k in e.get('extra_env_keys', [])}),
                  acceptance_eligible=bool(summary['complete'] and seam_eligible and release_match(identity, args.release_sha256, args.release_commit)))
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
    parser.add_argument('--product-version', required=True, help='the product line the engine must speak, e.g. 0.0.11')
    parser.add_argument('--release-sha256', help='independently obtained digest of the release binary')
    parser.add_argument('--release-commit', help='independently obtained commit of the release')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--invocation-id')
    parser.add_argument('--case', action='append', choices=tuple(CASES))
    args = parser.parse_args(argv)
    binary = bool(args.engine_bin)
    if binary and (args.source_repo or args.commit or args.python or not args.engine_sha256 or args.seam != 'none'):
        parser.error('a pinned binary needs --engine-sha256, no source options and --seam none')
    if not binary and (not all((args.source_repo, args.commit, args.python)) or args.engine_sha256):
        parser.error('source selection needs --source-repo, a full --commit and --python')
    report = run(args)
    keys = ('no_failures', 'all_passed', 'complete', 'subset', 'acceptance_eligible', 'failed', 'blocked', 'seam', 'extra_env_keys')
    print(json.dumps({k: report[k] for k in keys} | {'report': str(Path(args.output).absolute() / 'report.json')}))
    return 0 if report['complete'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
