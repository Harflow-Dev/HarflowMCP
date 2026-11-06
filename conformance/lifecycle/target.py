"""Pinned engine target and the explicit environment it is run with.

`binary equals release binary`: the command is the pinned binary itself (digest verified before every
operation) or an exact-commit source launcher. The environment is built from a closed allowlist; any other
key (a backend selector, an injection hook, an interpreter path) is refused, so no selector can widen what
the tested engine may do. A seam that needs extra environment must declare it, and the report then marks the
run as not eligible for acceptance.
"""
import hashlib
from pathlib import Path
import re

from ..oracles import (ConformanceError, read_regular, required_capabilities, strict_json, validate_reply,
                       validate_version_info)
from ..process import run_process

ENV_ALLOWLIST = frozenset(('PATH', 'HOME', 'XDG_CONFIG_HOME', 'XDG_CACHE_HOME', 'XDG_DATA_HOME', 'TMPDIR', 'LANG', 'LC_ALL',
                           'PYTHONDONTWRITEBYTECODE', 'HARFLOW_STATE_ROOT'))
PUBLIC_COMMANDS = frozenset(('run', 'resume', 'status', 'cancel', 'task-events', 'task-prompt', 'list', 'version-info', 'state-inspect'))
OPERATION_TIMEOUT = 30
CLEAN_CLEANUP = ('group_signalled', 'group_absent', 'group_quiescent_os_observed')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


class Target:
    """A command prefix plus the identity it must keep for the whole run, including the product line it must speak."""

    def __init__(self, command, identity, verify, product_version):
        self.command, self._verify = list(command), verify
        self.product_version = product_version
        self.identity = {**identity, 'product_version': product_version}

    def verify(self):
        self._verify()


def binary_target(path, sha256, product_version):
    path = Path(path)
    if not path.is_absolute() or not re.fullmatch('[a-f0-9]{64}', sha256 or ''):
        raise ConformanceError('an absolute path and a pinned digest are required')

    def verify():
        if digest(read_regular(path)) != sha256:
            raise ConformanceError('engine binary differs from its pinned digest')
    verify()
    return Target([str(path)], {'kind': 'binary', 'path': str(path), 'sha256': sha256, 'pinned_binary': True}, verify, product_version)


def source_target(command, identity, verify, product_version):
    """A pre-built source launcher (see conformance.e1.harness.source_target); never a pinned binary."""
    return Target(command, {**identity, 'pinned_binary': False}, verify, product_version)


def release_match(identity, release_sha256, release_commit):
    """Acceptance needs the pinned binary to equal an *independently supplied* release digest and commit."""
    return bool(identity.get('pinned_binary') and re.fullmatch('[a-f0-9]{64}', release_sha256 or '')
                and re.fullmatch('[a-f0-9]{40}', release_commit or '') and identity.get('sha256') == release_sha256)


def check_environment(env, extra=()):
    unknown = sorted(set(env) - ENV_ALLOWLIST - set(extra))
    if unknown:
        raise ConformanceError('environment key outside the allowlist (possible selector): ' + ', '.join(unknown))
    return env


class Engine:
    """Runs public ctl commands against a target in one isolated case, with a fresh handshake each time."""

    def __init__(self, target, env, cwd, output, extra_env_keys=()):
        self.target, self.cwd, self.output = target, Path(cwd), Path(output)
        self.extra_env_keys = tuple(extra_env_keys)
        self.env = check_environment(dict(env), self.extra_env_keys)
        self.records = []
        self.output.mkdir(parents=True, exist_ok=True)

    def _invoke(self, argv, timeout=OPERATION_TIMEOUT):
        self.target.verify()
        check_environment(self.env, self.extra_env_keys)
        record = run_process([*self.target.command, *argv], cwd=self.cwd, env=self.env,
                             artifact_dir=self.output / ('command-%04d' % len(self.records)), timeout=timeout)
        self.records.append(record)
        if (record.get('timed_out') or record.get('capture_exceeded') or not record.get('pipes_drained')
                or record.get('cleanup') not in CLEAN_CLEANUP or record.get('returncode') is None):
            raise ConformanceError('unknown bounded command outcome; never replayed')
        raw = read_regular(record['streams']['stdout']['path'])
        doc = strict_json(raw) if raw.strip() else None
        return {'document': doc, 'exit_code': record['returncode'], 'process': record}

    def command(self, argv):
        argv = list(argv)
        if not argv or argv[0] not in PUBLIC_COMMANDS or '--json' not in argv:
            raise ConformanceError('lifecycle command is not allowed or lacks --json')
        handshake = self._invoke(['version-info', '--json'])
        validate_version_info(handshake['document'], self.env['HARFLOW_STATE_ROOT'], required_capabilities(argv),
                              expected_version=self.target.product_version)
        reply = self._invoke(argv)
        if reply['document'] is not None:
            validate_reply(argv, reply['document'], self.target.product_version)
        return reply

    def clean(self):
        """Whether every command left a drained, signalled-or-absent process group."""
        return all(r.get('pipes_drained') and r.get('cleanup') in CLEAN_CLEANUP and not r.get('timed_out') for r in self.records)
