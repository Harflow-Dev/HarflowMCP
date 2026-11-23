"""The state of the selected root before a lifecycle case runs.

`missing` creates nothing (the runner never pre-creates the root). `empty` is an owner-only directory with no
entries. `own-lock-only` adds only the engine's own lock files, whose names differ by engine (delta tag
`own-lock-names`): Python `.harflow-store.lock`, Rust `store.lock` and `io.lock`. `stray` adds one file that
belongs to no engine. Each is created through the filesystem directly, never through an engine command.
"""
import os
from pathlib import Path

PRE_STATES = ('missing', 'empty', 'own-lock-only', 'stray')
OWN_LOCKS = {'python': ('.harflow-store.lock',), 'rust': ('store.lock', 'io.lock')}


def prepare_root(root, pre_state, engine):
    if pre_state == 'missing':
        return
    root = Path(root)
    root.mkdir(mode=0o700)
    root.chmod(0o700)
    if pre_state == 'own-lock-only':
        for name in OWN_LOCKS[engine]:
            _write(root / name, b'')
    elif pre_state == 'stray':
        _write(root / 'stranger.txt', b'not harflow')


def _write(path, data):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'wb') as stream:
        stream.write(data)
