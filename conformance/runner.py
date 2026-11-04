"""Explicit immutable-target S07 smoke orchestration; never launches an agent."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tarfile
import uuid

from .oracles import ConformanceError, PINNED_MANIFEST_SHA256, read_regular, strict_json, validate_envelope, validate_version_info, verify_manifest, validate_fake_observation
from .process import run_process


def digest(data):
    return hashlib.sha256(data).hexdigest()


def tree_hash(root):
    rows = []
    for path in sorted(Path(root).rglob('*')):
        if '__pycache__' in path.parts or path.suffix == '.pyc':
            continue
        if path.is_symlink():
            rows.append((str(path.relative_to(root)), 'link:' + os.readlink(path)))
        elif path.is_file():
            rows.append((str(path.relative_to(root)), digest(read_regular(path))))
    return digest(json.dumps(rows, separators=(',', ':')).encode())


def framework_identity():
    root = Path(__file__).parent
    return {path.name: digest(read_regular(path)) for path in sorted(root.glob('*.py'))}


def isolated_environment(case):
    case = Path(case).absolute()
    env = {'HOME': str(case / 'home'), 'XDG_CONFIG_HOME': str(case / 'xdg-config'),
           'XDG_CACHE_HOME': str(case / 'xdg-cache'), 'XDG_DATA_HOME': str(case / 'xdg-data'),
           'HARFLOW_STATE_ROOT': str(case / 'state'), 'PATH': str(case / 'bin') + ':/usr/bin:/bin',
           'LANG': 'C.UTF-8', 'LC_ALL': 'C.UTF-8', 'PYTHONDONTWRITEBYTECODE': '1',
           'TMPDIR': str(case / 'tmp')}
    for name in ('home', 'xdg-config', 'xdg-cache', 'xdg-data', 'bin', 'tmp', 'workspace'):
        (case / name).mkdir()
    return env


def publish_report(path, report):
    """Atomic no-clobber publication via same-directory hard link."""
    path = Path(path)
    temporary = path.parent / ('.report-' + uuid.uuid4().hex)
    raw = json.dumps(report, sort_keys=True, indent=2, allow_nan=False).encode()
    with open(temporary, 'xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path, follow_symlinks=False)
    finally:
        temporary.unlink()
    return path


def validate_report(path, expected_invocation_id):
    report = strict_json(read_regular(path))
    if not isinstance(report, dict) or report.get('format') != 'harflow.conformance.report' or type(report.get('v')) is not int or report['v'] != 1 or report.get('invocation_id') != expected_invocation_id:
        raise ConformanceError('report identity mismatch')
    context = strict_json(read_regular(Path(path).parent / 'context.json'))
    if context.get('invocation_id') != expected_invocation_id or context.get('manifest_sha256') != PINNED_MANIFEST_SHA256:
        raise ConformanceError('report context mismatch')
    root = Path(path).absolute().parent
    if strict_json(read_regular(root / 'framework-context.json')) != report.get('framework_sha256'):
        raise ConformanceError('framework context mismatch')
    if report.get('ok') is True:
        if report.get('manifest_sha256') != PINNED_MANIFEST_SHA256 or report.get('external_identity_before') != report.get('external_identity_after') or 'external_identity_before' not in report:
            raise ConformanceError('manifest or runtime identity mismatch')
        expected = [['version-info', '--json'], 'fresh-handshake', ['state-inspect', '--json']]
        if [case.get('operation') for case in report.get('cases', [])] != expected:
            raise ConformanceError('incomplete operation evidence')
        if strict_json(read_regular(root / 'target-context.json')) != report.get('target'):
            raise ConformanceError('target context mismatch')
        if report.get('target', {}).get('kind') == 'source' and not report['target'].get('runtime'):
            raise ConformanceError('missing runtime evidence')
        if [row.get('mode') for row in report.get('fake_cases', [])] != ['version', 'success', 'nonzero', 'hang', 'flood', 'pipe-holder']:
            raise ConformanceError('missing fake checks')
        for fake in report['fake_cases']:
            record = fake['process']
            validate_fake_observation(fake.get('fake_observation'), record)
            if record.get('cleanup') not in ('group_signalled', 'group_absent', 'group_quiescent_os_observed') or not record.get('pipes_drained') or record.get('returncode') is None:
                raise ConformanceError('incomplete fake cleanup')
            if (fake['mode'] in ('version', 'success') and (record.get('ok') is not True or record.get('returncode') != 0)) or (fake['mode'] == 'nonzero' and record.get('returncode') != 7) or (fake['mode'] == 'hang' and record.get('timed_out') is not True) or (fake['mode'] == 'flood' and record.get('capture_exceeded') is not True):
                raise ConformanceError('fake outcome mismatch')
    records = [(case['process'], True) for case in report.get('cases', [])]
    records += [(case['process'], False) for case in report.get('fake_cases', [])]
    for record, engine in records:
        if engine and report.get('ok') is True and (record.get('ok') is not True or record.get('returncode') != 0 or not record.get('pipes_drained') or record.get('cleanup') == 'unknown'):
            raise ConformanceError('failed process evidence')
        for stream in record['streams'].values():
            capture = Path(os.path.abspath(os.path.normpath(stream['path'])))
            if not capture.is_relative_to(root):
                raise ConformanceError('capture outside report')
            raw = read_regular(capture)
            if digest(raw) != stream['sha256'] or len(raw) != stream['captured_bytes'] or stream['bytes_seen'] < len(raw):
                raise ConformanceError('capture metadata mismatch')
        folder = Path(record['streams']['stdout']['path']).parent
        ledger = strict_json(read_regular(folder / 'ledger.json'))
        spawn = strict_json(read_regular(folder / 'spawn.json'))
        if ledger != record or any(spawn.get(key) != record.get(key) for key in ('nonce', 'pid', 'pgid', 'argv', 'cwd', 'environment_sha256')):
            raise ConformanceError('ledger or spawn mismatch')
    return report


def validate_operation(argv):
    if list(argv) not in (['version-info', '--json'], ['state-inspect', '--json']):
        raise ConformanceError('S07 operation is not allowed')


def _runtime(python, folder, env):
    binary = Path(python).resolve(strict=True)
    # Query identity in isolation, never importing the engine or caller environment.
    script = 'import json,sys,sysconfig,platform; print(json.dumps({"version":sys.version,"platform":sys.platform,"architecture":platform.machine(),"soabi":sysconfig.get_config_var("SOABI"),"purelib":sysconfig.get_path("purelib"),"stdlib":sysconfig.get_path("stdlib")}))'
    record = run_process([str(python), '-I', '-B', '-c', script], cwd=folder, env=env, artifact_dir=folder / 'runtime-probe')
    if not record['ok']:
        raise ConformanceError('runtime identity probe failed')
    details = strict_json(read_regular(record['streams']['stdout']['path']))
    details.update({'launcher': str(python), 'binary': str(binary), 'binary_sha256': digest(read_regular(binary)),
                    'site_packages_sha256': tree_hash(Path(details['purelib']))})
    return details


def _snapshot(repo, commit, destination):
    if not re.fullmatch('[a-f0-9]{40}', commit):
        raise ConformanceError('full immutable commit required')
    archive = destination.parent / 'source.tar'
    with open(archive, 'xb') as stream:
        subprocess.run(['/usr/bin/git', '-c', 'safe.directory=' + str(repo), '-C', str(repo), 'archive', '--format=tar', commit], stdout=stream, stderr=subprocess.PIPE, check=True,
                       env={'PATH': '/usr/bin:/bin', 'HOME': str(destination.parent / 'home'), 'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null'})
    destination.mkdir()
    with tarfile.open(archive) as bundle:
        for member in bundle.getmembers():
            path = Path(member.name)
            if path.is_absolute() or '..' in path.parts or not (member.isfile() or member.isdir()):
                raise ConformanceError('unsafe archive entry')
        bundle.extractall(destination)
    return {'commit': commit, 'archive_sha256': digest(read_regular(archive)), 'tree_sha256': tree_hash(destination)}


def _execute(output, manifest, manifest_sha256, target_factory, fingerprint, invocation_id=None):
    output = Path(output).absolute()
    output.mkdir(exist_ok=False)
    invocation_id = invocation_id or uuid.uuid4().hex
    if not re.fullmatch('[a-f0-9]{32}', invocation_id):
        raise ConformanceError('invalid invocation id')
    with open(output / 'context.json', 'x') as stream:
        json.dump({'invocation_id': invocation_id, 'manifest_sha256': manifest_sha256}, stream)
    framework_before = framework_identity()
    with open(output / 'framework-context.json', 'x') as stream:
        json.dump(framework_before, stream, sort_keys=True)
    report = {'framework_sha256': framework_before, 'format': 'harflow.conformance.report', 'v': 1, 'invocation_id': invocation_id,
              'ok': False, 'cases': [], 'limitations': ['S07 only: no lifecycle or agent qualification', 'Owned process groups only; escaped descendants are not contained', 'Hashes establish observed identity, not authenticity or system-library identity']}
    try:
        verify_manifest(manifest, manifest_sha256)
        report['manifest_sha256'] = manifest_sha256
        case = output / 'smoke'
        case.mkdir()
        env = isolated_environment(case)
        command, identity, check_identity = target_factory(output, case, env)
        report['target'] = identity
        with open(output / 'target-context.json', 'x') as stream:
            json.dump(identity, stream, sort_keys=True)
        fake_python = identity.get('runtime', {}).get('launcher', sys.executable)
        report['fake_cases'] = [run_fake_case(fake_python, mode, output / ('fake-' + mode), timeout=2) for mode in ('version', 'success', 'nonzero', 'hang', 'flood', 'pipe-holder')]
        for fake in report['fake_cases']:
            record = fake['process']
            if record['cleanup'] == 'unknown' or not record.get('pipes_drained') or (fake['mode'] in ('version', 'success') and not record['ok']) or (fake['mode'] == 'nonzero' and record.get('returncode') != 7) or (fake['mode'] == 'hang' and not record['timed_out']) or (fake['mode'] == 'flood' and not record['capture_exceeded']):
                raise ConformanceError('fake infrastructure check failed')
        before = fingerprint()
        report['external_identity_before'] = before
        for index, operation in enumerate((['version-info', '--json'], ['state-inspect', '--json'])):
            validate_operation(operation)
            if operation[0] != 'version-info':
                probe = run_process(command + ['version-info', '--json'], cwd=case / 'workspace', env=env, artifact_dir=case / ('probe-' + str(index)))
                if not probe['ok']:
                    raise ConformanceError('fresh handshake process failed')
                validate_version_info(strict_json(read_regular(probe['streams']['stdout']['path'])), env['HARFLOW_STATE_ROOT'], ('ctl.v1', 'state-recovery.v1'), sys.platform, platform.machine())
                report['cases'].append({'operation': 'fresh-handshake', 'process': probe})
            result = run_process(command + operation, cwd=case / 'workspace', env=env, artifact_dir=case / ('operation-' + str(index)))
            report['cases'].append({'operation': operation, 'process': result})
            if not result['ok']:
                raise ConformanceError('engine process failed')
            doc = strict_json(read_regular(result['streams']['stdout']['path']))
            if operation[0] == 'version-info':
                validate_version_info(doc, env['HARFLOW_STATE_ROOT'], ('ctl.v1', 'state-recovery.v1'), sys.platform, platform.machine())
            else:
                validate_envelope(doc, 'harflow.ctl')
                row = doc.get('result', {})
                if row.get('state_root') != env['HARFLOW_STATE_ROOT'] or row.get('status') != 'missing' or row.get('reason') != 'missing_root' or row.get('cooperative_ownership_only') is not True or 'recovery' in row or 'error' in doc:
                    raise ConformanceError('unexpected missing-root inspection')
            if os.path.lexists(env['HARFLOW_STATE_ROOT']):
                raise ConformanceError('read-only operation created state')
        check_identity()
        after = fingerprint()
        report['external_identity_after'] = after
        if before != after:
            raise ConformanceError('external runtime/source drift')
        if framework_identity() != framework_before:
            raise ConformanceError('framework source drift')
        report['ok'] = True
    except Exception as exc:
        try:
            report['external_identity_after'] = fingerprint()
        except Exception as identity_exc:
            report['identity_error'] = str(identity_exc)
        report['error'] = type(exc).__name__ + ': ' + str(exc)
    publish_report(output / 'report.json', report)
    validate_report(output / 'report.json', invocation_id)
    return report


def run_suite(source_repo, commit, python, manifest, manifest_sha256, output, invocation_id=None):
    repo, python = Path(source_repo).absolute(), Path(python).absolute()
    if not python.is_file():
        raise ConformanceError('explicit Python required')
    def factory(folder, case, env):
        snapshot = folder / 'source'
        source = _snapshot(repo, commit, snapshot)
        runtime = _runtime(python, folder, env)
        original = tree_hash(snapshot)
        src = snapshot / 'src'
        bootstrap = ('import importlib.util,pathlib,runpy,sys; '
                     'sys.path.insert(0,' + repr(str(src)) + '); '
                     's=importlib.util.find_spec("harflow_mcp.cli"); '
                     'assert s and s.origin and pathlib.Path(s.origin).resolve().is_relative_to(pathlib.Path(' + repr(str(src)) + ').resolve()); '
                     'runpy.run_module("harflow_mcp.cli",run_name="__main__")')
        def check():
            if tree_hash(snapshot) != original:
                raise ConformanceError('snapshot drift')
            # Recompute runtime bytes, without repeating the identity subprocess.
            if digest(read_regular(Path(runtime['binary']))) != runtime['binary_sha256'] or tree_hash(Path(runtime['purelib'])) != runtime['site_packages_sha256']:
                raise ConformanceError('runtime drift')
        return [str(python), '-I', '-B', '-c', bootstrap], {'kind': 'source', 'source': source, 'runtime': runtime}, check
    return _execute(output, manifest, manifest_sha256, factory, lambda: {'source_src_sha256': tree_hash(repo / 'src'), 'python_sha256': digest(read_regular(python.resolve()))}, invocation_id)


def run_binary_suite(engine_bin, engine_sha256, manifest, manifest_sha256, output, invocation_id=None):
    binary = Path(engine_bin)
    if not binary.is_absolute() or not re.fullmatch('[a-f0-9]{64}', engine_sha256 or ''):
        raise ConformanceError('absolute explicitly pinned binary required')
    def fingerprint():
        value = digest(read_regular(binary))
        if value != engine_sha256:
            raise ConformanceError('binary digest mismatch')
        return value
    fingerprint()
    def factory(folder, case, env):
        return [str(binary)], {'kind': 'binary', 'path': str(binary), 'sha256': engine_sha256}, fingerprint
    return _execute(output, manifest, manifest_sha256, factory, fingerprint, invocation_id)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-repo')
    parser.add_argument('--commit')
    parser.add_argument('--python')
    parser.add_argument('--engine-bin', default=os.environ.get('HARFLOW_ENGINE_BIN'))
    parser.add_argument('--engine-sha256')
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--manifest-sha256', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--invocation-id')
    args = parser.parse_args(argv)
    if args.engine_bin:
        if args.source_repo or args.commit or args.python or not args.engine_sha256:
            parser.error('binary selection requires digest and excludes source selection')
        report = run_binary_suite(args.engine_bin, args.engine_sha256, args.manifest, args.manifest_sha256, args.output, args.invocation_id)
    else:
        if not all((args.source_repo, args.commit, args.python)) or args.engine_sha256:
            parser.error('source selection requires source-repo, full commit and python')
        report = run_suite(args.source_repo, args.commit, args.python, args.manifest, args.manifest_sha256, args.output, args.invocation_id)
    print(json.dumps({'ok': report['ok'], 'invocation_id': report['invocation_id'], 'report': str(Path(args.output).absolute() / 'report.json')}))
    return 0 if report['ok'] else 1


def run_fake_case(python, mode, case, timeout=2, capture_limit=65536):
    """Directly supervise the standalone fake; never route it through the engine."""
    from .oracles import validate_fake_observation
    case = Path(case).absolute()
    case.mkdir(exist_ok=False)
    env = isolated_environment(case)
    nonce = uuid.uuid4().hex
    observation = case / 'fake-observation.json'
    script = Path(__file__).with_name('fake_harness.py').absolute()
    argv = [str(python), str(script), mode, '--nonce', nonce, '--observation', str(observation)]
    record = run_process(argv, cwd=case / 'workspace', env=env, artifact_dir=case / 'capture', timeout=timeout, capture_limit=capture_limit, nonce=nonce)
    claim = strict_json(read_regular(observation))
    validate_fake_observation(claim, record)
    return {'mode': mode, 'process': record, 'fake_observation': claim, 'fake_sha256': digest(read_regular(script))}
