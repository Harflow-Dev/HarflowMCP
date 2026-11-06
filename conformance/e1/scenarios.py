"""Run the E1 read scenarios against a pinned engine target.

    python -m conformance.e1.scenarios --source-repo REPO --commit FULL_SHA --python PYTHON \\
        --manifest contracts/v1/manifest.json --manifest-sha256 DIGEST --output NEW_DIR

    python -m conformance.e1.scenarios --engine-bin /abs/engine --engine-sha256 DIGEST \\
        --manifest contracts/v1/manifest.json --manifest-sha256 DIGEST --output NEW_DIR

The source target is a Git archive of the exact commit, run from an isolated snapshot. The binary
target is a caller-pinned digest. Neither selects an editable checkout or searches PATH.
"""
import argparse
import json
import os
from pathlib import Path
import platform
import re
import sys
import traceback
import uuid

from ..oracles import ConformanceError, PINNED_MANIFEST_SHA256, read_regular, strict_json, verify_manifest
from ..runner import digest, framework_identity, publish_report
from . import cases as case_module
from .harness import Ctx, binary_target, source_target


def run_cases(command, identity, check, output, selected=None, probe=False):
    results = []
    for name, (capabilities, function) in case_module.CASES.items():
        if selected and name not in selected:
            continue
        ctx = Ctx(name, Path(output) / 'cases' / name, command, capabilities, probe=probe)
        try:
            function(ctx)
        except Exception as exc:  # a crashing case is a failure, never a pass
            ctx.failures.append('case raised ' + type(exc).__name__ + ': ' + str(exc))
            ctx.failures.append(traceback.format_exc(limit=4))
        outcome = ctx.finish()
        outcome['checks_detail'] = ctx.checks
        results.append(outcome)
        print(('PASS ' if outcome['ok'] else 'FAIL ') + name + f' ({outcome["checks"]} checks)', file=sys.stderr)
        for failure in outcome['failures']:
            print('   ' + failure.splitlines()[0], file=sys.stderr)
    return results


def run(args):
    output = Path(args.output).absolute()
    output.mkdir(exist_ok=False)
    invocation_id = args.invocation_id or uuid.uuid4().hex
    if not re.fullmatch('[a-f0-9]{32}', invocation_id):
        raise ConformanceError('invalid invocation id')
    manifest = verify_manifest(args.manifest, args.manifest_sha256)
    report = {'format': 'harflow.conformance.e1-report', 'v': 1, 'invocation_id': invocation_id, 'ok': False,
              'manifest_sha256': args.manifest_sha256, 'fixture_count': len(manifest['files']),
              'framework_sha256': framework_identity(), 'platform': [sys.platform, platform.machine()], 'cases': []}
    try:
        if args.engine_bin:
            command, identity, check, _ = binary_target(args.engine_bin, args.engine_sha256)
        else:
            command, identity, check, _ = source_target(args.source_repo, args.commit, args.python, output)
        report['target'] = identity
        check()
        report['cases'] = run_cases(command, identity, check, output, args.case, args.probe)
        check()
        known = set(case_module.CASES)
        missing = sorted(set(args.case or ()) - known)
        if missing:
            raise ConformanceError('unknown case: ' + ', '.join(missing))
        report['ok'] = bool(report['cases']) and all(case['ok'] for case in report['cases'])
        report['complete'] = not args.case and len(report['cases']) == len(known)
    except Exception as exc:
        report['error'] = type(exc).__name__ + ': ' + str(exc)
    publish_report(output / 'report.json', report)
    return report


def validate_report(path, expected_invocation_id):
    """Re-read a report and check every recorded operation against its retained process ledger."""
    path = Path(path)
    report = strict_json(read_regular(path))
    if report.get('format') != 'harflow.conformance.e1-report' or report.get('invocation_id') != expected_invocation_id:
        raise ConformanceError('report identity mismatch')
    root = path.parent
    for case in report['cases']:
        for operation in case['operations']:
            folder = Path(operation['process'])
            if not folder.is_relative_to(root):
                raise ConformanceError('operation outside report')
            ledger = strict_json(read_regular(folder / 'ledger.json'))
            for stream in ledger['streams'].values():
                raw = read_regular(stream['path'])
                if digest(raw) != stream['sha256'] or len(raw) != stream['captured_bytes']:
                    raise ConformanceError('capture metadata mismatch')
            if ledger.get('returncode') != operation['rc']:
                raise ConformanceError('operation status mismatch')
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--source-repo')
    parser.add_argument('--commit')
    parser.add_argument('--python')
    parser.add_argument('--engine-bin', default=os.environ.get('HARFLOW_ENGINE_BIN'))
    parser.add_argument('--engine-sha256')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--invocation-id')
    parser.add_argument('--case', action='append', help='run only this case; repeat for several (a subset is not the complete matrix)')
    parser.add_argument('--probe', action='store_true', help='print every command result while running')
    parser.add_argument('--list', action='store_true', help='list case names and exit')
    args = parser.parse_args(argv)
    if args.list:
        print('\n'.join(case_module.CASES))
        return 0
    if args.engine_bin:
        if args.source_repo or args.commit or args.python or not args.engine_sha256:
            parser.error('binary selection requires --engine-sha256 and excludes source selection')
    elif not all((args.source_repo, args.commit, args.python)) or args.engine_sha256:
        parser.error('source selection requires --source-repo, a full --commit and --python')
    report = run(args)
    validate_report(Path(args.output).absolute() / 'report.json', report['invocation_id'])
    print(json.dumps({'ok': report['ok'], 'complete': report.get('complete'), 'invocation_id': report['invocation_id'],
                      'report': str(Path(args.output).absolute() / 'report.json'),
                      'failed': [case['name'] for case in report['cases'] if not case['ok']],
                      **({'error': report['error']} if 'error' in report else {})}))
    return 0 if report['ok'] and report.get('complete') else 1


if __name__ == '__main__':
    raise SystemExit(main())
