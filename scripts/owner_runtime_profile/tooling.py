"""Bounded deterministic candidate inventory generation and verification."""
import hashlib
import os
from pathlib import Path
import re
import tempfile

from . import classify, fixtures
from .cases import canonical

SPEC_NAMES = ('owner-runtime-v1.md', 'owner-runtime-v1-schema.md')
MANIFEST_LIMIT = 131072

def digest(raw):
    return hashlib.sha256(raw).hexdigest()

def read_bounded(path, limit):
    if path.is_symlink() or not path.is_file():
        raise ValueError('missing, symlink or nonregular file: ' + str(path))
    with path.open('rb') as stream:
        raw = stream.read(limit + 1)
    if len(raw) > limit:
        raise ValueError('file exceeds budget: ' + str(path))
    return raw

def safe_directory(path):
    path = Path(path)
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('symlink directory')
    if path.exists() and not path.is_dir():
        raise ValueError('output is not a directory')
    return path

def expected_assets(specification_paths):
    paths = tuple(Path(path) for path in specification_paths)
    if len(paths) != 2 or tuple(path.name for path in paths) != SPEC_NAMES:
        raise ValueError('exact specification pair required')
    cases = fixtures()
    if len(cases) > 64:
        raise ValueError('too many cases')
    entries = []
    assets = {}
    for name, (raw, expected) in sorted(cases.items()):
        if not re.fullmatch(r'[a-z0-9_-]+\.json', name) or name == 'manifest.json':
            raise ValueError('unsafe specimen filename')
        if classify(raw) != expected:
            raise ValueError('independent oracle disagreement: ' + name)
        assets[name] = raw
        entries.append({'filename': name, 'bytes': len(raw), 'sha256': digest(raw), 'expected': expected})
    manifest = {'format': 'harflow.owner-runtime.profile.component', 'v': 1,
                'profile': 'owner-runtime-v1', 'profile_version': 1,
                'status': 'specified_unimplemented', 'provenance': 'synthetic_specification',
                'python_runtime_support': False,
                'specifications': [{'path': 'docs/decisions/' + path.name,
                                    'sha256': digest(read_bounded(path, MANIFEST_LIMIT))} for path in paths],
                'files': entries}
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
            if (entry.is_symlink() or not entry.is_file(follow_symlinks=False)
                    or entry.stat(follow_symlinks=False).st_nlink != 1):
                raise ValueError('nonregular candidate entry')
            present.add(entry.name)
    if not allow_missing and names - present:
        raise ValueError('candidate inventory mismatch')

def write_atomic(path, raw):
    """Replace a candidate entry without truncating an existing inode."""
    descriptor, temporary = tempfile.mkstemp(prefix='.owner-runtime-', dir=path.parent)
    temporary = Path(temporary)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            os.fchmod(stream.fileno(), 0o644)
            stream.write(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def generate(output_dir, specification_paths):
    """Write only owned expected files, refusing extra inventory; never delete."""
    directory = safe_directory(output_dir)
    assets = expected_assets(specification_paths)
    inventory(directory, set(assets), True)
    directory.mkdir(parents=True, exist_ok=True)
    for name, raw in assets.items():
        write_atomic(directory / name, raw)

def check(output_dir, specification_paths):
    """Read-only exact inventory, bounded byte/hash and independent oracle check."""
    directory = safe_directory(output_dir)
    assets = expected_assets(specification_paths)
    inventory(directory, set(assets), False)
    for name, expected in assets.items():
        limit = MANIFEST_LIMIT if name == 'manifest.json' else (24577 if name.endswith('.json') else 65)
        if read_bounded(directory / name, limit) != expected:
            raise ValueError('candidate bytes differ: ' + name)
