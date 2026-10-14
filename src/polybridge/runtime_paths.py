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


# Bounds cover the combined, deduplicated effective authority per validation.
WRITABLE_SCAN_MAX_ENTRIES = 100_000
WRITABLE_SCAN_MAX_DEPTH = 128
WRITABLE_SCAN_SECONDS = 10.0


def assert_single_link_writable_files(scopes: list[tuple[Path, str]]) -> None:
    """Refuse pre-existing aliases to files outside the sandbox's writable roots.

    The OS denies new cross-boundary hardlinks, but an existing writable alias can
    mutate its read-only source inode. Conservatively reject every regular file
    with multiple links, even when all aliases might be inside authorized roots.
    Directory symlinks are never followed; their in-scope targets are scanned by
    their direct names. Errors and exhausted traversal bounds refuse authority.
    """
    import stat
    import time

    deadline = time.monotonic() + WRITABLE_SCAN_SECONDS
    entries = 0
    roots = []
    for path, kind in sorted(set(scopes), key=lambda item: len(item[0].parts)):
        if any(parent_kind == 'directory' and path.is_relative_to(parent)
               for parent, parent_kind in roots):
            continue
        roots.append((path, kind))
    seen_directories = set()

    observed_directories = []
    observed_roots = []

    def check(info, depth):
        nonlocal entries
        entries += 1
        if entries > WRITABLE_SCAN_MAX_ENTRIES or depth > WRITABLE_SCAN_MAX_DEPTH:
            raise ValueError('writable hardlink scan exceeds entry or depth bound')
        if time.monotonic() > deadline:
            raise ValueError('writable hardlink scan exceeds time budget')
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise ValueError('existing multiply-linked regular files in writable authority are not supported')

    def identity(info):
        return info.st_dev, info.st_ino

    def visit(parent_fd, name, path, depth, expected_kind=None):
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        check(info, depth)
        if expected_kind is not None and not (
            stat.S_ISDIR(info.st_mode) if expected_kind == 'directory' else stat.S_ISREG(info.st_mode)
        ):
            raise ValueError('writable scope type changed during hardlink scan')
        if expected_kind is not None:
            observed_roots.append((path, identity(info)))
        if not stat.S_ISDIR(info.st_mode):
            return
        inode = identity(info)
        if inode in seen_directories:
            raise ValueError('writable hardlink scan encountered a directory cycle')
        seen_directories.add(inode)
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
        try:
            if identity(os.fstat(fd)) != inode:
                raise ValueError('writable directory identity changed during hardlink scan')
            observed_directories.append((path, inode))
            # Ancestor descriptors remain open while descendants are traversed.
            # Every stat/open is anchored to the same parent as its enumeration.
            with os.scandir(fd) as children:
                for child in children:
                    visit(fd, child.name, path / child.name, depth + 1)
            if identity(os.stat(name, dir_fd=parent_fd, follow_symlinks=False)) != inode:
                raise ValueError('writable directory identity changed during hardlink scan')
        finally:
            os.close(fd)

    try:
        for path, kind in roots:
            # Canonical scope ancestors must also be opened without following a
            # concurrently substituted symlink. Close each ancestor after opening
            # the next one; the resulting parent descriptor anchors this root.
            if len(path.parts) > WRITABLE_SCAN_MAX_DEPTH:
                raise ValueError('writable hardlink scan exceeds entry or depth bound')
            fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                for part in path.parts[1:-1]:
                    if time.monotonic() > deadline:
                        raise ValueError('writable hardlink scan exceeds time budget')
                    next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    os.close(fd)
                    fd = next_fd
                visit(fd, path.name, path, 0, kind)
            finally:
                os.close(fd)
        # Descriptor traversal cannot accidentally scan a replacement elsewhere.
        # Also refuse a renamed/replaced lexical tree before accepting authority.
        for path, inode in observed_directories + observed_roots:
            if time.monotonic() > deadline:
                raise ValueError('writable hardlink scan exceeds time budget')
            if path.resolve(strict=True) != path or identity(path.lstat()) != inode:
                raise ValueError('writable directory identity changed during hardlink scan')
    except OSError as exc:
        raise ValueError('writable hardlink scan requires accessible, stable entries') from exc
