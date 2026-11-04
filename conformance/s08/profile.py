"""Controlled immutable source profile with an explicitly recorded fixture overlay."""
import json
import os
from pathlib import Path
import shlex
import shutil
import sys

from conformance.oracles import (ConformanceError, read_regular, strict_json,
    validate_envelope, validate_version_info, verify_manifest)
from conformance.process import run_process
from conformance.runner import _snapshot, _runtime, digest, tree_hash, framework_identity
from .observer import Observer

ALLOWED = frozenset(('run', 'task', 'status', 'resume', 'send', 'cancel-message', 'cancel',
    'approval-list', 'approval-get', 'approval-respond', 'task-events', 'task-prompt',
    'task-list-page', 'version-info', 'state-inspect'))

def framework_hash():
    # Both the overlay/observer code and the shared controller helpers execute.
    identity = {'s08': tree_hash(Path(__file__).parent), 'shared': framework_identity()}
    return digest(json.dumps(identity, sort_keys=True).encode())


class SourceProfile:
    allowed_commands = ALLOWED

    def overlay_sources(self):
        return {name: Path(__file__).parent / name for name in
                ("bootstrap.py", "backend.py", "fake_agent.py", "client.py")}

    def make_observer(self):
        return Observer(self.output / "observer")

    def framework_hash(self):
        return framework_hash()

    def __init__(self, repo, commit, output, python=None, *, tasks_enabled=False):
        self.repo, self.commit = Path(repo), commit
        self.output = Path(output).absolute()
        self.python = Path(python or sys.executable).absolute()
        self.tasks_enabled = tasks_enabled
        self.records = []
        self.observer = None

    def __enter__(self):
        self.output.mkdir(exist_ok=False)
        self.source = self.output / 'source'
        self.workspace = self.output / 'workspace'
        self.state_root = self.output / 'state'
        for name in ('home', 'workspace', 'state', 'fake-ledger', 'overlay', 'tmp',
                     'xdg-config', 'xdg-cache', 'xdg-data'):
            (self.output / name).mkdir()
        self.env = {'PATH': '/usr/bin:/bin', 'HOME': str(self.output / 'home'),
            'HARFLOW_STATE_ROOT': str(self.state_root), 'PYTHONDONTWRITEBYTECODE': '1',
            'TMPDIR': str(self.output / 'tmp'),
            'XDG_CONFIG_HOME': str(self.output / 'xdg-config'),
            'XDG_CACHE_HOME': str(self.output / 'xdg-cache'),
            'XDG_DATA_HOME': str(self.output / 'xdg-data')}
        self.source_identity = _snapshot(self.repo, self.commit, self.source)
        verify_manifest(self.source / 'contracts/v1/manifest.json')
        self.runtime_identity = _runtime(self.python, self.output, self.env)
        overlay = self.output / 'overlay'
        for name, source in self.overlay_sources().items():
            shutil.copyfile(source, overlay / name)
        self.shim = overlay / 'python-shim'
        self.shim.write_text('#!/bin/sh\nexec ' + shlex.quote(str(self.python)) +
            ' -I -B ' + shlex.quote(str(overlay / 'bootstrap.py')) + ' "$@"\n')
        self.shim.chmod(0o700)
        self.observer = self.make_observer().start()
        try:
            self.env.update(S08_OBSERVER_SOCKET=self.observer.socket_path,
                S08_OBSERVER_NONCE=self.observer.nonce, S08_ARCHIVE_SRC=str(self.source / 'src'),
                S08_REAL_PYTHON=str(self.python), S08_PYTHON_SHIM=str(self.shim),
                S08_FAKE_LEDGER=str(self.output / 'fake-ledger'), S08_WORKSPACE=str(self.workspace))
            self.identity = {'kind': 'controlled_source_fixture_injection',
                'stock_backend_qualification': False, 'source': self.source_identity,
                'runtime': self.runtime_identity, 'pristine_source_sha256': tree_hash(self.source),
                'active_profile_sha256': tree_hash(overlay),
                'framework_sha256': self.framework_hash(),
                'environment_sha256': digest(json.dumps(self.env, sort_keys=True).encode()),
                'shim': str(self.shim)}
            self.initial_fingerprint = self.fingerprint()
            (self.output / 'injection-manifest.json').write_text(json.dumps(self.identity, indent=2, sort_keys=True))
        except BaseException:
            try:
                clean = self.observer.cleanup(timeout=5)
            finally:
                self.observer.close()
            (self.output / 'cleanup.json').write_text(json.dumps({'ok': clean, 'during_setup': True}))
            raise
        return self

    def fingerprint(self):
        return {'source': tree_hash(self.source), 'overlay': tree_hash(self.output / 'overlay'),
            'runtime_binary': digest(read_regular(self.runtime_identity['binary'])),
            'runtime_site': tree_hash(Path(self.runtime_identity['purelib'])),
            'environment': digest(json.dumps(self.env, sort_keys=True).encode()),
            'selectors': digest(json.dumps({'python': str(self.python), 'shim': str(self.shim),
                'workspace': str(self.workspace), 'source': str(self.source),
                'state_root': str(self.state_root)}, sort_keys=True).encode()),
            'framework': self.framework_hash()}

    def _invoke(self, argv):
        before = self.fingerprint()
        if before != self.initial_fingerprint:
            raise ConformanceError('profile fingerprint differs from initial injection identity')
        record = run_process([str(self.shim), *argv], cwd=self.workspace, env=self.env,
            artifact_dir=self.output / ('command-%04d' % len(self.records)), timeout=15)
        self.records.append(record)
        if before != self.fingerprint():
            raise ConformanceError('profile fingerprint changed during operation')
        if (record.get('timed_out') or record.get('capture_exceeded')
                or not record.get('pipes_drained') or record.get('cleanup') not in
                ('group_signalled', 'group_absent', 'group_quiescent_os_observed')
                or record.get('returncode') is None):
            raise ConformanceError('unknown bounded command outcome; never replay')
        raw = read_regular(record['streams']['stdout']['path'])
        doc = strict_json(raw)
        validate_envelope(doc, 'harflow.version-info' if argv[0] == 'version-info' else 'harflow.ctl')
        return {'document': doc, 'exit_code': record['returncode'], 'process': record}

    def command(self, argv, requiredcap=(), *, required_capabilities=None):
        argv = list(argv)
        if not argv or argv[0] not in self.allowed_commands or '--json' not in argv:
            raise ConformanceError('S08 command is not allowed or lacks JSON')
        if argv[0] not in ('version-info', 'state-inspect') and not self.tasks_enabled:
            raise ConformanceError('task execution requires verified observer gate')
        handshake = self._invoke(['version-info', '--json'])
        caps = requiredcap if required_capabilities is None else required_capabilities
        if isinstance(caps, str):
            caps = (caps,)
        validate_version_info(handshake['document'], self.state_root, caps,
            expected_os=self.runtime_identity['platform'],
            expected_arch=self.runtime_identity['architecture'])
        return handshake if argv[0] == 'version-info' else self._invoke(argv)

    def __exit__(self, exc_type, exc, tb):
        clean = self.observer.cleanup(timeout=5) if self.observer else True
        if self.observer:
            self.observer.close()
        (self.output / 'cleanup.json').write_text(json.dumps({'ok': clean}))
        if not clean and exc is None:
            raise ConformanceError('observer participant cleanup unproved')
