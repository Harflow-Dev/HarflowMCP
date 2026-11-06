"""Case context and target construction for the E1 read scenarios.

Standard library only. The engine under test is a black box reached through its public
command line, in a fresh isolated root per case. Expectations live in `cases.py` as literals
derived from `seed.py` and the published contract text.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

from ..oracles import ConformanceError, read_regular, strict_json, validate_envelope, validate_version_info
from ..process import run_process
from ..runner import _runtime, _snapshot, digest, isolated_environment, tree_hash
from . import world
from .port import PythonLayoutAdapter

OPERATION_TIMEOUT = 60
# Harness policy for a cold index: how long a client re-issues a read that reports preparation.
BOOTSTRAP_ATTEMPTS = 40
BOOTSTRAP_INTERVAL_SECONDS = 0.25
TAGS = ('[contract]', '[baseline', '[harness]', '[proposed')


class Failure(Exception):
    """A violated expectation; collected, not fatal to the remaining checks of a case."""


class Result:
    def __init__(self, args, record, stdout, stderr):
        self.args, self.record, self.stdout, self.stderr = list(args), record, stdout, stderr
        self.rc = record.get('returncode')
        self.doc = None
        self.parse_error = None
        text = stdout.decode('utf-8', 'replace').strip()
        if text:
            try:
                self.doc = strict_json(text)
            except ConformanceError as exc:
                self.parse_error = 'stdout is not exactly one JSON document: ' + str(exc)

    @property
    def result(self):
        return (self.doc or {}).get('result')

    @property
    def error(self):
        return (self.doc or {}).get('error')

    def summary(self):
        return {'args': self.args, 'rc': self.rc, 'stdout_bytes': len(self.stdout),
                'doc': self.doc, 'stderr': self.stderr.decode('utf-8', 'replace')[:300]}


class Ctx:
    """One case: a fresh isolated root, a way to run the engine, and recorded checks."""

    def __init__(self, name, folder, command, capabilities, probe=False, port=None, default_tag='baseline'):
        self.port = port or PythonLayoutAdapter()
        self.default_tag = default_tag
        self.skips = []
        self.name, self.folder, self.command, self.capabilities = name, Path(folder), list(command), capabilities
        self.probe = probe
        self.folder.mkdir(parents=True)
        self.env = isolated_environment(self.folder)
        self.root = Path(self.env['HARFLOW_STATE_ROOT'])
        self.tasks = self.root / 'tasks'
        self.checks = []
        self.failures = []
        self.operations = []
        self.baseline = None
        self.fx = Bound(self.port, self.root)
        self.handshake_done = False
        self._count = 0

    # --- seeding and invariants -------------------------------------------------------------------

    def seed_reads(self):
        self.fx.seed_world(world.WORLD_READS)
        self.rebase()

    def init_root(self):
        self.fx.initialize_empty()
        self.rebase()

    def digest(self, root=None):
        value = self.port.authoritative_digest(root or self.root)
        if not isinstance(value, str) or len(value) < 32:
            raise Failure('the fixture adapter returned an empty authoritative digest')
        return value

    def rebase(self):
        """Declare the current authoritative state as the expected unchanged state.

        The digest must be non-empty and, once the world is seeded, differ from the digest of an empty
        initialized store, so an adapter that hashes nothing cannot make the invariant vacuous."""
        self.baseline = self.digest()
        if not getattr(self, '_empty_digest', None):
            scratch = self.folder / 'empty-store'
            self.port.initialize_empty(scratch)
            self._empty_digest = self.digest(scratch)
        if self.baseline != self._empty_digest:
            self._nonvacuous = True

    def skip(self, message):
        self.skips.append(message)

    # --- running the engine -----------------------------------------------------------------------

    def handshake(self):
        if self.handshake_done:
            return
        record = run_process(self.command + ['version-info', '--json'], cwd=self.folder / 'workspace', env=self.env,
                             artifact_dir=self.folder / 'op-handshake', timeout=OPERATION_TIMEOUT)
        out = read_regular(record['streams']['stdout']['path'])
        try:
            validate_version_info(strict_json(out), self.env['HARFLOW_STATE_ROOT'], self.capabilities, sys.platform, None)
            self.check(True, 'fresh handshake qualifies the required capabilities', tag='harness')
        except ConformanceError as exc:
            self.check(False, f'fresh handshake failed: {exc}', tag='harness')
        self.handshake_done = True

    def ctl(self, *args, env=None, expect_rc=None, auto=True):
        """Run one public command and apply the checks every document must satisfy."""
        self.handshake()
        self._count += 1
        environment = dict(self.env)
        for key, value in (env or {}).items():
            if value is None:
                environment.pop(key, None)
            else:
                environment[key] = value
        folder = self.folder / f'op-{self._count:03d}'
        record = run_process(self.command + [str(a) for a in args], cwd=self.folder / 'workspace', env=environment,
                             artifact_dir=folder, timeout=OPERATION_TIMEOUT)
        stdout = read_regular(record['streams']['stdout']['path'])
        stderr = read_regular(record['streams']['stderr']['path'])
        result = Result(args, record, stdout, stderr)
        self.operations.append({'args': list(map(str, args)), 'rc': result.rc, 'process': str(folder)})
        if auto:
            self.document_checks(result)
        if expect_rc is not None:
            self.check(result.rc == expect_rc, f'{args[0]} exit status {result.rc} == {expect_rc}', tag='baseline')
        if self.probe:
            print(json.dumps(result.summary(), sort_keys=True))
        return result

    def document_checks(self, res):
        name = ' '.join(map(str, res.args[:2]))
        clean = (res.record.get('pipes_drained') and not res.record.get('timed_out')
                 and not res.record.get('capture_exceeded') and res.record.get('cleanup') != 'unknown')
        self.check(clean, f'{name}: process finished, drained and was cleaned up', tag='harness')
        self.check(res.rc in (0, 1, 2), f'{name}: exit status {res.rc} is 0, 1 or 2', tag='contract')
        if '--json' in map(str, res.args) or res.rc == 2 and res.stdout:
            if res.rc != 2 or res.stdout:
                self.check(res.parse_error is None and res.doc is not None, f'{name}: stdout is one strict JSON document ({res.parse_error})', tag='contract')
        if res.doc is None:
            return
        try:
            validate_envelope(res.doc, 'harflow.ctl')
            self.check(True, f'{name}: common envelope', tag='contract')
        except ConformanceError as exc:
            self.check(False, f'{name}: common envelope: {exc}', tag='contract')
        if res.rc == 0:
            self.check('error' not in res.doc, f'{name}: success carries no error', tag='contract')
        else:
            error = res.doc.get('error')
            self.check(isinstance(error, dict) and isinstance(error.get('code'), str) and isinstance(error.get('message'), str),
                       f'{name}: failure carries error.code and error.message', tag='contract')
            if res.rc == 2:
                self.check(isinstance(error, dict) and error.get('code') == 'usage', f'{name}: exit 2 is a usage error', tag='contract')
        self.check(len(res.stdout) <= 1_048_576, f'{name}: document within 1 MiB', tag='harness')

    # --- assertions -------------------------------------------------------------------------------

    def check(self, condition, message, tag=None):
        if not any(t in message for t in TAGS):
            message = f'{message} [{tag or self.default_tag}]'
        self.checks.append({'ok': bool(condition), 'message': message})
        if not condition:
            self.failures.append(message)
        return bool(condition)

    def eq(self, actual, expected, message):
        ok = actual == expected
        self.check(ok, message if ok else f'{message}: expected {expected!r}, got {actual!r}')
        return ok

    def finish(self):
        if self.baseline is not None:
            self.check(getattr(self, '_nonvacuous', False), 'authoritative digest distinguishes a seeded store from an empty one', tag='harness')
            self.eq(self.digest(), self.baseline, 'authoritative state unchanged by reads [harness]')
        return {'name': self.name, 'ok': not self.failures, 'failures': self.failures, 'skips': self.skips,
                'checks': len(self.checks), 'operations': self.operations}


class Bound:
    """The fixture port bound to one state root: `ctx.fx.add_task(spec)`."""

    def __init__(self, port, root):
        self._port, self._root = port, root

    def at(self, root):
        return Bound(self._port, root)

    def __getattr__(self, name):
        method = getattr(self._port, name)
        return lambda *args, **kwargs: method(self._root, *args, **kwargs)
def source_target(repo, commit, python, output):
    """Python source at an exact commit, run from an isolated archive (never an editable checkout)."""
    repo, python, output = Path(repo).absolute(), Path(python).absolute(), Path(output).absolute()
    snapshot = output / 'source'
    source = _snapshot(repo, commit, snapshot)
    env = isolated_environment_for_probe(output)
    runtime = _runtime(python, output, env)
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
        if digest(read_regular(Path(runtime['binary']))) != runtime['binary_sha256']:
            raise ConformanceError('runtime drift')
    identity = {'kind': 'source', 'source': source, 'runtime': runtime}
    return [str(python), '-I', '-B', '-c', bootstrap], identity, check, snapshot


def isolated_environment_for_probe(output):
    probe = Path(output) / 'runtime-env'
    probe.mkdir()
    return isolated_environment(probe)


def binary_target(engine_bin, engine_sha256):
    binary = Path(engine_bin)
    if not binary.is_absolute() or not re.fullmatch('[a-f0-9]{64}', engine_sha256 or ''):
        raise ConformanceError('absolute explicitly pinned binary required')

    def check():
        if digest(read_regular(binary)) != engine_sha256:
            raise ConformanceError('binary digest mismatch')
    check()
    return [str(binary)], {'kind': 'binary', 'path': str(binary), 'sha256': engine_sha256}, check, None

