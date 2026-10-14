"""Protect the server's execution namespace from bounded writable authority."""
import os
import re
from pathlib import Path
import shutil
import shlex
import sys
import sysconfig


# Server repository probes, outcome capture, process identity and Monitor launch.
HELPERS = ('git', 'ps', 'open')


def _chain(path: Path) -> tuple[set[Path], Path]:
    """Keep lexical components as well as every symlink hop and final target."""
    path = path.absolute()
    pending = list(path.parts[1:])
    current = Path(path.anchor)
    protected = set()
    hops = 0
    while pending:
        part = pending.pop(0)
        if part == '..':
            current = current.parent
            continue
        if part == '.':
            continue
        current /= part
        protected.add(current)
        if current.is_symlink():
            hops += 1
            if hops > 40:
                raise ValueError('server runtime symlink chain is not bounded')
            target = Path(os.readlink(current))
            current = Path(target.anchor) if target.is_absolute() else current.parent
            pending = (list(target.parts[1:]) if target.is_absolute() else list(target.parts)) + pending
    return protected, current


def assert_runtime_outside_writes(scopes: list[tuple[Path, str]]) -> None:
    """Reject authority capable of replacing helpers or introducing PATH helpers.

    No subprocess is executed. Recompute on each preflight/final check so ambient
    PATH changes cannot silently expand the server's trusted execution namespace.
    """
    from . import backends
    directories = []
    components = set()
    executables = []
    linked_inodes = set()
    for entry in os.get_exec_path():
        lexical = Path(entry or os.curdir).absolute()
        chain, canonical = _chain(lexical)
        components.update(chain)
        directories.extend((lexical, canonical))
    for name in (*HELPERS, *(backend.binary for backend in backends.BACKENDS.values())):
        selected = shutil.which(name)
        if selected:
            executables.append(Path(selected))
    executables.append(Path(sys.executable))
    seen = set()
    while executables:
        executable = executables.pop()
        if executable in seen:
            continue
        seen.add(executable)
        if len(seen) > 64:
            raise ValueError('server runtime interpreter chain is not bounded')
        chain, target = _chain(executable)
        components.update(chain)
        # Installed script entry points may import code outside their PATH bin
        # directory. Protect their discovered virtualenv/package container, not
        # an assumed complete dependency graph.
        for parents in (executable.absolute().parents, target.parents):
            for parent in parents:
                if (parent / 'pyvenv.cfg').is_file() or (parent / 'package.json').is_file():
                    chain, container = _chain(parent)
                    components.update(chain)
                    directories.extend((parent, container))
                    break
        # A venv interpreter may resolve into a separate uv/pyenv installation.
        # Its standard library lives beside bin, outside the venv and this server's
        # base_prefix. Protect recognized installation layouts without executing it.
        if target.parent.name == 'bin' and re.fullmatch(r'python(?:[0-9]+(?:\.[0-9]+)*)?[td]?', target.name):
            installation = target.parent.parent
            directories.append(installation)
        with target.open('rb') as stream:
            header = stream.read(4096)
        if header.startswith(b'#!'):
            line = header.split(b'\n', 1)[0]
            if len(line) == 4096:
                raise ValueError('server runtime shebang exceeds safe bound')
            try:
                words = shlex.split(line[2:].decode('utf-8'))
            except (UnicodeError, ValueError) as exc:
                raise ValueError('server runtime shebang cannot be qualified') from exc
            if not words or not Path(words[0]).is_absolute():
                raise ValueError('server runtime interpreter must be absolute')
            interpreter = Path(words[0])
            executables.append(interpreter)
            if interpreter.name == 'env':
                args = words[1:]
                split_args = bool(args and args[0] == '-S')
                if split_args:
                    args = args[1:]
                elif len(args) != 1:
                    raise ValueError('server runtime env shebang cannot be qualified')
                if not args or args[0].startswith('-') or '=' in args[0]:
                    raise ValueError('server runtime env shebang cannot be qualified')
                selected = shutil.which(args[0])
                if not selected:
                    raise ValueError('server runtime env interpreter is unavailable')
                executables.append(Path(selected))
        info = target.stat()
        linked_inodes.add((info.st_dev, info.st_ino))
        if info.st_nlink != 1 and not (os.statvfs(target).f_flag & os.ST_RDONLY):
            raise ValueError('server runtime executable hardlinks on mutable filesystems are not supported with extra writable paths')
    # The interpreter, imports and installed server code execute outside the
    # worker sandbox. Granting these roots would undermine subsequent requests.
    for root in (sys.prefix, sys.base_prefix, str(Path(__file__).parent),
                 sysconfig.get_path('stdlib'), sysconfig.get_path('purelib'), sysconfig.get_path('platlib'),
                 *(entry or os.curdir for entry in sys.path)):
        if root:
            lexical = Path(root).absolute()
            chain, canonical = _chain(lexical)
            components.update(chain)
            directories.extend((lexical, canonical))
    for scope, kind in scopes:
        if any(scope == path or (kind == 'directory' and path.is_relative_to(scope)) for path in components):
            raise ValueError('writable authority overlaps protected server runtime paths')
        if any(scope.is_relative_to(root) or (kind == 'directory' and root.is_relative_to(scope)) for root in directories):
            raise ValueError('writable authority overlaps protected server runtime namespace')
        info = scope.stat()
        if (info.st_dev, info.st_ino) in linked_inodes:
            raise ValueError('writable authority overlaps protected server runtime executable hardlink')
