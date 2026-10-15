"""Protect the server's execution namespace from bounded writable authority."""
import os
import re
from pathlib import Path
import shutil
import shlex
import sys
import sysconfig

from .canonical_paths import canonical_component_path


# Server repository probes, outcome capture, process identity and Monitor launch.
HELPERS = ('git', 'ps', 'open')

# The Monitor app is a fixed launch target: `PB_OPEN_MONITOR=1` opens exactly
# this bundle (`open -g -a <absolute path>`), never LaunchServices' mutable
# URL-handler mapping. Its whole namespace — absent names and symlink targets
# included — is protected by `assert_runtime_outside_writes` regardless of
# `PB_OPEN_MONITOR`, so no writable authority can replace what a later
# ordinary launch of this bundle would open.
MONITOR_BUNDLE = Path('/Applications/Polybridge Monitor.app')


def _chain(path: Path) -> tuple[set[Path], Path]:
    """Keep lexical components as well as every symlink hop and final target.

    Each existing component is recorded in the kernel's actual spelling
    (case and Unicode form), looked up without following it when it is a
    symlink; missing components stay lexical after their verified ancestors.
    On case-insensitive filesystems this is what makes a differently-spelled
    alias of a protected component recognizable instead of silently passing.
    """
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
        try:
            actual = canonical_component_path(current)
        except OSError as exc:
            raise ValueError('protected path spelling cannot be verified') from exc
        if actual is not None:
            # Preserve the lexical hop, then follow only the kernel's nofollow
            # spelling. Earlier symlinks were already visited; the current one
            # remains a symlink, while firmlink aliases now share one target.
            protected.add(current.with_name(actual.name))
            current = actual
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
    if os.environ.get('PB_OPEN_MONITOR') == '1':
        raise ValueError('extra writable paths require Monitor auto-open disabled; open Monitor manually')
    directories = []
    components = set()
    executables = []
    loader_executables = []
    linked_inodes = set()
    for entry in os.get_exec_path():
        # Worker subprocesses use their repository cwd. Relative/empty entries
        # would therefore resolve differently from this server's qualification.
        if not entry or not Path(entry).is_absolute():
            raise ValueError('extra writable paths require absolute nonempty PATH entries')
        lexical = Path(entry)
        chain, canonical = _chain(lexical)
        components.update(chain)
        directories.extend((lexical, canonical))
    for name in (*HELPERS, *(backend.binary for backend in backends.BACKENDS.values())):
        selected = shutil.which(name)
        if selected:
            executables.append(Path(selected))
            with Path(selected).open('rb') as stream:
                script = stream.read(2) == b'#!'
            if script:
                raise ValueError('selected script runtime dependency graphs are not qualified for extra writable paths')
            if name in HELPERS or not script:
                loader_executables.append(Path(selected))
    executables.append(Path(sys.executable))
    from .runtime_loaders import qualify_loaders
    def protect_loaders(paths):
        # Interpreters discovered from shebangs need the same native dependency
        # closure as initially selected helpers. Merely protecting their binary
        # leaves mutable dylibs outside the interpreter container exposed.
        loader_files, loader_roots = qualify_loaders(paths)
        for dependency in loader_files:
            chain, _ = _chain(dependency)
            components.update(chain)
        executables.extend(path for path in loader_files if path.exists())
        for root in loader_roots:
            chain, canonical = _chain(root)
            components.update(chain)
            directories.extend((root, canonical))

    protect_loaders([*loader_executables, Path(sys.executable)])
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
            protect_loaders([interpreter])
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
                protect_loaders([Path(selected)])
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
    # The Monitor bundle namespace is protected whole — every existing
    # component of its path plus symlink targets — whether or not the bundle
    # or `PB_OPEN_MONITOR` is present, so a later ordinary launch of the
    # fixed bundle cannot have been redirected by writable authority.
    monitor_chain, monitor_target = _chain(MONITOR_BUNDLE)
    components.update(monitor_chain)
    directories.extend((MONITOR_BUNDLE, monitor_target))
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
