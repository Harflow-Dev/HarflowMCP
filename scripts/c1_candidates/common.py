"""Shared envelope grammar, canonical bytes and deterministic generation/verification for C1 candidates."""
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

from owner_runtime_profile.validation import Rejected, decode, require, supported

FORMAT = 'harflow.c1.candidate'
COMPONENT = 'harflow.c1.candidate.component'
MANIFEST_LIMIT = 131072
SPECIMEN_LIMIT = 24576
MAX_FILES = 64


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2, allow_nan=False) + '\n').encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def specimen(candidate, kind, document, input=None):
    value = {'format': FORMAT, 'v': 1, 'candidate': candidate, 'kind': kind, 'document': document}
    if input is not None:
        value['input'] = input
    return value


def classify(candidate, raw):
    """Independent oracle: the result an engine adopting this candidate must reach for these bytes."""
    try:
        value = decode(raw)
        require(isinstance(value, dict) and value.get('format') == FORMAT and type(value.get('v')) is int
                and isinstance(value.get('kind'), str) and isinstance(value.get('document'), dict)
                and value.get('candidate') == candidate['name'], 'envelope')
        supported(value['v'] == 1 and value['kind'] in candidate['kinds'], 'envelope')
        return candidate['evaluate'](value['kind'], value)
    except Rejected as rejected:
        return rejected.result


def enums(data, spec, booleans=()):
    require(isinstance(data, dict), 'fields')
    require(all(isinstance(data.get(key), str) for key in spec), 'fields')
    require(all(type(data.get(key)) is bool for key in booleans), 'fields')
    supported(all(data[key] in choices.split() for key, choices in spec.items()), 'enum')


def safe_directory(path):
    path = Path(path)
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('symlink directory')
    if path.exists() and not path.is_dir():
        raise ValueError('output is not a directory')
    return path


def read_bounded(path, limit):
    if path.is_symlink() or not path.is_file():
        raise ValueError('missing, symlink or nonregular file: ' + str(path))
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('file exceeds budget: ' + str(path))
    return raw


def expected_assets(candidate, repo):
    cases = candidate['cases']()
    if len(cases) > MAX_FILES:
        raise ValueError('too many cases')
    entries, assets = [], {}
    for name, (value, expected) in sorted(cases.items()):
        raw = value if isinstance(value, bytes) else canonical(value)
        if not re.fullmatch(r'[a-z0-9_-]+\.json', name) or name == 'manifest.json':
            raise ValueError('unsafe specimen filename')
        if classify(candidate, raw) != expected:
            raise ValueError('independent oracle disagreement: ' + name)
        assets[name] = raw
        entries.append({'filename': name, 'bytes': len(raw), 'sha256': digest(raw), 'expected': expected})
    specs = [{'path': path, 'sha256': digest(read_bounded(Path(repo) / path, MANIFEST_LIMIT))} for path in candidate['specs']]
    manifest = {'format': COMPONENT, 'v': 1, 'candidate': candidate['name'], 'capability': candidate['capability'],
                'status': 'specified_unimplemented', 'provenance': 'synthetic_specification',
                'python_runtime_support': False, 'advertised': False, 'decision_basis': candidate['decision_basis'],
                'specifications': specs, 'files': entries}
    assets['manifest.json'] = canonical(manifest)
    if len(assets['manifest.json']) > MANIFEST_LIMIT:
        raise ValueError('manifest exceeds budget')
    assets['manifest.sha256'] = (digest(assets['manifest.json']) + '\n').encode('ascii')
    return assets


def inventory(directory, names, allow_missing):
    if not directory.exists():
        if allow_missing:
            return
        raise ValueError('missing candidate directory')
    present = set()
    with os.scandir(directory) as entries:
        for index, entry in enumerate(entries):
            if index >= len(names) or entry.name not in names:
                raise ValueError('candidate inventory mismatch')
            if entry.is_symlink() or not entry.is_file(follow_symlinks=False) or entry.stat(follow_symlinks=False).st_nlink != 1:
                raise ValueError('nonregular candidate entry')
            present.add(entry.name)
    if not allow_missing and names - present:
        raise ValueError('candidate inventory mismatch')


def write_atomic(path, raw):
    descriptor, temporary = tempfile.mkstemp(prefix='.c1-candidate-', dir=path.parent)
    temporary = Path(temporary)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o644)
            stream.write(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def generate(candidate, repo, output=None):
    directory = safe_directory(output or Path(repo) / candidate['directory'])
    assets = expected_assets(candidate, repo)
    inventory(directory, set(assets), True)
    directory.mkdir(parents=True, exist_ok=True)
    for name, raw in assets.items():
        write_atomic(directory / name, raw)


def check(candidate, repo, output=None):
    directory = safe_directory(output or Path(repo) / candidate['directory'])
    assets = expected_assets(candidate, repo)
    inventory(directory, set(assets), False)
    for name, expected in assets.items():
        limit = MANIFEST_LIMIT if name == 'manifest.json' else (SPECIMEN_LIMIT if name.endswith('.json') else 65)
        if read_bounded(directory / name, limit) != expected:
            raise ValueError('candidate bytes differ: ' + name)
