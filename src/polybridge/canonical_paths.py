"""Kernel-canonical path spellings on case-insensitive filesystems.

`Path.resolve` keeps the spelling a caller supplied, so on a case-insensitive
or Unicode-normalizing filesystem a differently-spelled alias of an existing
path resolves "successfully" without ever naming the path the kernel would
actually touch — a grant requested as `CONFIG.TOML` would resolve to the
protected `config.toml` under a spelling no protection recognizes. Grants and
protected namespaces must therefore be compared in the kernel's own spelling.

On Darwin, `F_GETPATH` (fcntl 50) on an event-only descriptor reports the
actual on-disk case and Unicode form of what was opened. A `nofollow` open
(`O_SYMLINK`) keeps a symlink's own name instead of its target's; a following
open reports the fully resolved target. The descriptor is always closed.

Capability is detected from the `open` flags actually available, never from
`sys.platform`: tests monkeypatch the platform to "darwin" on Linux, where
the Darwin constants — and the fcntl itself — do not exist. Where the
capability is absent, ordinary strict `Path.resolve` behaviour is retained
and no Darwin fcntl is issued. Lookup failures fail closed: an unexplained
error refuses authority rather than falling back to the supplied spelling.
"""
import errno
import fcntl
import os
from pathlib import Path

# Darwin fcntl command number for F_GETPATH; the buffer matches PATH_MAX.
F_GETPATH = 50
GETPATH_BYTES = 1024

_KERNEL_CANONICAL_SUPPORTED = hasattr(os, "O_EVTONLY") and hasattr(os, "O_SYMLINK")


def kernel_canonical_supported() -> bool:
    """Whether kernel-canonical spellings are actually available here."""
    return _KERNEL_CANONICAL_SUPPORTED


def _fgetpath(path: Path, nofollow: bool) -> str:
    # O_NONBLOCK keeps the probe from ever waiting on a FIFO's writer; the
    # descriptor is only used for F_GETPATH, never for I/O.
    flags = os.O_EVTONLY | os.O_NONBLOCK | (os.O_SYMLINK if nofollow else 0)
    fd = os.open(path, flags)
    try:
        raw = fcntl.fcntl(fd, F_GETPATH, b"\0" * GETPATH_BYTES)
    finally:
        os.close(fd)
    return os.fsdecode(raw.split(b"\0", 1)[0])


def _valid_utf8(spelling: str, what: str) -> str:
    try:
        spelling.encode("utf-8")
    except UnicodeError as exc:
        raise ValueError(f"kernel {what} is not valid UTF-8") from exc
    return spelling


def canonical(path: Path) -> Path:
    """The kernel's actual spelling of an existing path, symlinks followed.

    Equivalent to strict resolution, but the result carries the case and
    Unicode form the filesystem actually stores, so a case-alias of a
    protected name cannot pass comparisons under the supplied spelling.
    Missing paths and lookup errors refuse (fail closed).
    """
    if not kernel_canonical_supported():
        return path.resolve(strict=True)
    result = Path(_valid_utf8(_fgetpath(path, nofollow=False), "path spelling"))
    if not result.is_absolute():
        raise ValueError("kernel path spelling is not absolute")
    return result


def canonical_component_path(path: Path) -> Path | None:
    """Full kernel spelling of a component, without following its final symlink.

    The lookup never follows the final symlink, so a link component keeps its
    own name rather than its target's. Earlier components have already been
    walked by the caller, which must retain each lexical hop before using this
    physical path. This also collapses filesystem firmlink aliases. Missing
    components (ENOENT) stay lexical with their ancestors'
    verified spelling; every other failure is raised so authority fails
    closed. Without the kernel capability this is always None, keeping the
    supplied spelling exactly as before.
    """
    if not kernel_canonical_supported():
        return None
    try:
        spelling = _fgetpath(path, nofollow=True)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            return None
        raise
    result = Path(_valid_utf8(spelling, "path component spelling"))
    if not result.is_absolute() or not result.name:
        raise ValueError("kernel path spelling is empty")
    return result


def canonical_component_name(path: Path) -> str | None:
    result = canonical_component_path(path)
    return result.name if result is not None else None
