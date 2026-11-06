"""The fixture port: the only way the E1 cases create, damage or inspect engine state.

A case never writes a file under the state root. It calls one of the named operations below, and an
adapter turns the layout-neutral request into whatever store the engine under test uses (the engine
may keep a SQLite root; no file layout is part of this interface).

Operations (all take the state root first):

    initialize_empty(root)                    a compatible, empty, initialized store
    remove_root(root)                         the selected root no longer exists
    make_foreign_root(root)                   a nonempty root that is not a Harflow store
    root_state(root) -> 'missing' | 'initialized' | 'other'
    seed_world(root, world)                   the layout-neutral world from `world.py`
    add_task(root, spec) -> dict              one settled or running task (see keys below)
    add_live_caller(root, task_id, pid, identity='alive', status='running', marker=None) -> dict
    damage_task(root, task_id)                task metadata that is no longer a valid task
    append_event(root, task_id, seq, kind, observed_at, fields)
    append_malformed_line(root, task_id)
    append_partial_line(root, task_id)        an unterminated trailing event line
    truncate_log(root, task_id, keep_events)
    remove_log(root, task_id)
    tamper_prompt(root, task_id)              the stored full prompt no longer matches its digest
    write_artifacts(root, task_id, items, header_valid=True) -> dict
    change_artifact_target(root, task_id, name)
    authoritative_digest(root) -> str         non-empty digest of all authoritative state

`add_task` spec keys: task_id, status, exit_code, started_at, finished_at, prompt, title, group, model,
session_id, continuation_of, output_drained, prompt_store ('inline' | 'sidecar_verified' |
'sidecar_missing' | 'sidecar_mismatch' | 'sidecar_foreign_name'), real_workspace (bool).

`add_live_caller` identities. Required of every adapter: 'alive' (the pid's current start identity matches
the record), 'reused' (the pid now has a different start identity, so the record is dead) and 'unknown'
(the identity cannot be decided either way). Optional, Python-record forms: 'legacy_alive_with_markers',
'legacy_markers_missing', 'legacy_markers_seen', 'legacy_markers_unseen', 'legacy_no_markers'. `status` is
'running', 'completed_observed' (terminal with an exit code) or 'failed_unobserved' (terminal without one).
It returns `{'supported': bool}`. An unsupported required state fails the scenario; an unsupported
`legacy_*` state is a recorded skip.

Every `add_task` spec is complete: `backend`, `repo_path`, `freedom`, `session_id`, `status`, `exit_code`,
`started_at`, `finished_at`, `output_drained` and `prompt` are always present (see `world.task`); optional
keys are `title`, `group`, `model`, `continuation_of`, `prompt_store` and `real_workspace`. An artifact
target is `<repo_path>/out/<name>` (`world.artifact_path`). `seed_world` takes the argument `world`.

An external adapter is an executable invoked as `ADAPTER OP --root ROOT` with the JSON arguments on
stdin and one JSON result on stdout; it must be pinned by SHA-256 and is never reachable from the
public engine binary.
"""
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess

from ..oracles import ConformanceError, read_regular
from . import seed, world as worldmod

OPERATIONS = ('initialize_empty', 'remove_root', 'make_foreign_root', 'root_state', 'seed_world', 'add_task',
              'add_live_caller', 'damage_task', 'append_event', 'append_malformed_line', 'append_partial_line',
              'truncate_log', 'remove_log', 'tamper_prompt', 'write_artifacts', 'change_artifact_target',
              'authoritative_digest')
DERIVED = ('.listing', '.task-list-revision', '.statistics')


def _lstart(pid):
    out = subprocess.run(['ps', '-o', 'lstart=', '-p', str(pid)], capture_output=True, text=True,
                         env={**os.environ, 'LC_ALL': 'C'}).stdout
    return ' '.join(out.split())


class PythonLayoutAdapter:
    """Adapter for Python 0.0.11's file layout. The only place that knows these file names."""
    name = 'python-0.0.11-layout'

    def identity(self):
        return {'name': self.name}

    def _tasks(self, root):
        return Path(root) / 'tasks'

    def initialize_empty(self, root):
        seed.init_root(root)

    def remove_root(self, root):
        shutil.rmtree(root, ignore_errors=True)

    def make_foreign_root(self, root):
        Path(root).mkdir()
        (Path(root) / 'stranger.txt').write_text('not harflow')

    def root_state(self, root):
        root = Path(root)
        if not os.path.lexists(root):
            return 'missing'
        try:
            marker = json.loads((root / '.harflow-store.json').read_text())
        except (OSError, ValueError):
            return 'other'
        if marker.get('format') != 'harflow.store' or not (root / '.harflow-store.lock').is_file():
            return 'other'
        return 'initialized'

    def seed_world(self, root, world):
        self.initialize_empty(root)
        for task in world['tasks']:
            self.add_task(root, task)
        for task_id, events in world['events'].items():
            for row in events:
                self.append_event(root, task_id, row['seq'], row['kind'], row['observed_at'], row['fields'])

    def add_task(self, root, spec):
        spec = dict(spec)
        task_id = spec.pop('task_id')
        tasks = self._tasks(root)
        # Every asserted value comes from the spec; a missing one is an error, never an adapter default.
        fields = {'backend': spec['backend'], 'repo_path': spec['repo_path'], 'freedom': spec['freedom'],
                  'session_id': spec['session_id'], 'status': spec['status'], 'exit_code': spec['exit_code'],
                  'started_at': spec['started_at'], 'finished_at': spec['finished_at'],
                  'output_drained': spec['output_drained'], 'prompt': spec['prompt']}
        for source, target in (('title', 'title'), ('group', 'group'), ('model', 'model')):
            if source in spec:
                fields[target] = spec[source]
        if spec.get('continuation_of'):
            fields.update(continuation_of_task_id=spec['continuation_of'], chain_root_task_id=spec['continuation_of'])
        result = {}
        if spec.get('real_workspace'):
            repo = Path(os.path.realpath(Path(root).parent)) / 'repos' / task_id
            repo.mkdir(parents=True, exist_ok=True)
            fields['repo_path'] = str(repo)
            result['repo_path'] = str(repo)
        store = spec.get('prompt_store', 'inline')
        if store != 'inline':
            text = fields['prompt']
            digest = hashlib.sha256(text.encode('utf-8')).hexdigest()
            fields['prompt'] = text[:worldmod.PREVIEW_CHARS]
            fields['prompt_truncated'] = True
            if store == 'sidecar_foreign_name':
                fields['prompt_source'] = 'bad-name.txt'
            elif store == 'sidecar_missing':
                fields['prompt_source'] = f'{task_id}.prompt.{hashlib.sha256(b"absent").hexdigest()}.txt'
            else:
                name = f'{task_id}.prompt.{digest}.txt'
                fields['prompt_source'] = name
                seed._write(tasks / name, text + ('tampered' if store == 'sidecar_mismatch' else ''))
        seed.write_task(tasks, task_id, **fields)
        return result

    def add_live_caller(self, root, task_id, pid, identity='alive', status='running', marker=None):
        start = _lstart(pid)
        fields = {'pid': pid, 'started_at': '2026-11-07T00:00:00+00:00', 'prompt': 'the caller'}
        table = {
            'alive': {'start_time': start, 'markers': []},
            'legacy_alive_with_markers': {'start_time': start, 'markers': [marker]},
            'reused': {'start_time': 'Mon Jan  1 00:00:00 2001', 'markers': []},
            'unknown': {'start_time': start, 'markers': ['no-such-marker-in-any-command-line']},
            'legacy_markers_missing': {'start_time': start, 'markers': ['no-such-marker-in-any-command-line']},
            'legacy_markers_seen': {'start_time': None, 'markers': [marker]},
            'legacy_markers_unseen': {'start_time': None, 'markers': ['no-such-marker-in-any-command-line']},
            'legacy_no_markers': {'start_time': None, 'markers': []},
        }
        if identity not in table or (identity in ('legacy_alive_with_markers', 'legacy_markers_seen') and not marker):
            return {'supported': False}
        fields.update(table[identity])
        states = {'running': dict(status='running', exit_code=None, finished_at=None, output_drained=False),
                  'completed_observed': dict(status='completed', exit_code=0, finished_at='2026-11-07T00:01:00+00:00', output_drained=True),
                  'failed_unobserved': dict(status='failed', exit_code=None, finished_at='2026-11-07T00:01:00+00:00', output_drained=False)}
        fields.update(states[status])
        seed.write_task(self._tasks(root), task_id, **fields)
        return {'supported': True}

    def damage_task(self, root, task_id):
        path = self._tasks(root) / f'{task_id}.meta.json'
        if path.exists():
            path.unlink()
        seed._write(path, json.dumps({'task_id': task_id, 'product': 'other', 'format': 'x', 'v': 1,
                                      'product_version': seed.PRODUCT_VERSION}))

    def _log(self, root, task_id):
        return self._tasks(root) / f'{task_id}.events.jsonl'

    def append_event(self, root, task_id, seq, kind, observed_at, fields):
        line = seed.event(seq, kind, task_id, observed_at, **fields)
        with open(self._log(root, task_id), 'ab') as stream:
            stream.write((json.dumps(line, ensure_ascii=False) + '\n').encode('utf-8'))
        os.chmod(self._log(root, task_id), 0o600)

    def append_malformed_line(self, root, task_id):
        with open(self._log(root, task_id), 'ab') as stream:
            stream.write(b'not json\n')

    def append_partial_line(self, root, task_id):
        with open(self._log(root, task_id), 'ab') as stream:
            stream.write(b'{"partial":')

    def truncate_log(self, root, task_id, keep_events):
        path = self._log(root, task_id)
        lines = path.read_bytes().splitlines(keepends=True)[:keep_events]
        path.write_bytes(b''.join(lines))

    def remove_log(self, root, task_id):
        self._log(root, task_id).unlink()

    def tamper_prompt(self, root, task_id):
        for path in self._tasks(root).glob(f'{task_id}.prompt.*.txt'):
            with open(path, 'ab') as stream:
                stream.write(b'tampered')

    def write_artifacts(self, root, task_id, items, header_valid=True):
        meta = json.loads((self._tasks(root) / f'{task_id}.meta.json').read_text())
        repo = Path(meta['repo_path'])
        rows, paths = [], []
        for item in items:
            path = Path(worldmod.artifact_path(str(repo), item['name']))
            if item.get('target') == 'present':
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b'artifact bytes')
                info = os.stat(path)
                observation = {'device': info.st_dev, 'inode': info.st_ino, 'size': info.st_size, 'mtime_ns': info.st_mtime_ns}
            else:
                observation = {'device': 1, 'inode': 2, 'size': 3, 'mtime_ns': 4}
            paths.append(str(path))
            rows.append({'artifact_id': item['artifact_id'], 'task_id': task_id, 'path': str(path), 'kind': item.get('kind', 'report'),
                         'note': item.get('note', 'n'), 'registered_at': '2026-01-10T10:04:00+00:00',
                         'provenance': {'method': 'verified_process_lineage', 'task_id': task_id},
                         'registered_observation': observation})
        document = {'product': 'harflow', 'format': 'harflow.task.artifacts' if header_valid else 'harflow.other', 'v': 1,
                    'product_version': seed.PRODUCT_VERSION, 'task_id': task_id, 'items': rows}
        target = self._tasks(root) / f'{task_id}.artifacts.json'
        if target.exists():
            target.unlink()
        seed._write(target, json.dumps(document))
        return {'paths': paths}

    def change_artifact_target(self, root, task_id, name):
        meta = json.loads((self._tasks(root) / f'{task_id}.meta.json').read_text())
        with open(Path(meta['repo_path']) / 'out' / name, 'ab') as stream:
            stream.write(b' changed')

    def authoritative_digest(self, root):
        """Every regular file under the root except derived caches and lock files."""
        root = Path(root)
        h = hashlib.sha256()
        for path in sorted(root.rglob('*')):
            if not path.is_file() or path.name.endswith('.lock') or any(path.name.startswith(p) for p in DERIVED):
                continue
            h.update(str(path.relative_to(root)).encode() + b'\0' + hashlib.sha256(path.read_bytes()).digest())
        return h.hexdigest()


class ExternalAdapter:
    """A pinned external fixture tool speaking the JSON protocol described in the module docstring."""

    def __init__(self, path, sha256):
        self.path = Path(path)
        self.sha256 = sha256
        self.name = 'external:' + sha256[:12]
        self.verify()

    def verify(self):
        if not self.path.is_absolute() or not re.fullmatch('[a-f0-9]{64}', self.sha256 or '') \
                or hashlib.sha256(read_regular(self.path)).hexdigest() != self.sha256:
            raise ConformanceError('fixture adapter must be an absolute path matching its pinned digest')

    def identity(self):
        return {'name': self.name, 'path': str(self.path), 'sha256': self.sha256}

    def call(self, op, root, **arguments):
        self.verify()
        done = subprocess.run([str(self.path), op, '--root', str(root)], input=json.dumps(arguments).encode(),
                              capture_output=True, timeout=120, env={'PATH': '/usr/bin:/bin'})
        if done.returncode != 0:
            raise ConformanceError(f'fixture adapter {op} failed: {done.stderr.decode(errors="replace")[:200]}')
        return json.loads(done.stdout) if done.stdout.strip() else None


def _external(op):
    def method(self, root, *args, **kwargs):
        names = {'seed_world': ('world',), 'add_task': ('spec',), 'add_live_caller': ('task_id', 'pid', 'identity', 'status', 'marker'),
                 'damage_task': ('task_id',), 'append_event': ('task_id', 'seq', 'kind', 'observed_at', 'fields'),
                 'append_malformed_line': ('task_id',), 'append_partial_line': ('task_id',), 'truncate_log': ('task_id', 'keep_events'),
                 'remove_log': ('task_id',), 'tamper_prompt': ('task_id',), 'write_artifacts': ('task_id', 'items', 'header_valid'),
                 'change_artifact_target': ('task_id', 'name')}.get(op, ())
        arguments = dict(zip(names, args))
        arguments.update(kwargs)
        return self.call(op, root, **arguments)
    return method


for _op in OPERATIONS:
    setattr(ExternalAdapter, _op, _external(_op))
