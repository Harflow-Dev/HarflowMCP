"""Cooperative, process-local capabilities for the Harflow state root.

Acquire the root before any inner storage lock. Context propagation carries a
capability, but does not extend its lifetime: background jobs must retain it.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import errno
import fcntl
import json
import math
import os
from pathlib import Path
import stat
import threading
import time
from typing import Any, Callable, Coroutine, Iterator, TypeVar

from .state_layout import StateLayout
from .wire_contracts import PRODUCT, PRODUCT_VERSION, compatible_line

MARKER_NAME = ".harflow-store.json"
LOCK_NAME = ".harflow-store.lock"
PENDING_NAME = ".harflow-store.json.pending"
MAX_MARKER_BYTES = 16_384
DEFAULT_TIMEOUT = 2.0


class StateOwnershipError(Exception):
    """Ownership failure that must never be suppressed as missing state."""


class StateIncompatible(StateOwnershipError):
    code = "state_incompatible"


class StateBusy(StateOwnershipError):
    code = "state_busy"


def _incompatible(detail: str) -> StateIncompatible:
    return StateIncompatible(f"Unsupported state root: {detail}; select a fresh state root")


def _identity(info: os.stat_result) -> tuple[int, int]:
    return info.st_dev, info.st_ino


def _regular(path: Path) -> os.stat_result | None:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        raise _incompatible(f"{path.name} must be a regular file, not a symlink")
    return info


def _marker(root: Path) -> bool:
    path = root / MARKER_NAME
    info = _regular(path)
    if info is None:
        return False
    if info.st_size > MAX_MARKER_BYTES:
        raise _incompatible("store marker is oversized")
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        if _identity(os.fstat(fd)) != _identity(info):
            raise _incompatible("store marker changed while opening")
        raw = os.read(fd, MAX_MARKER_BYTES + 1)
    finally:
        os.close(fd)
    try:
        value = json.loads(raw)
        if (len(raw) > MAX_MARKER_BYTES or not isinstance(value, dict)
                or value.get("product") != PRODUCT
                or value.get("format") != "harflow.store"
                or type(value.get("v")) is not int or value["v"] != 1
                or type(value.get("layout_version")) is not int
                or value["layout_version"] != 1
                or not compatible_line(value.get("product_version"), PRODUCT_VERSION)):
            raise ValueError("incompatible identity or version")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise _incompatible("invalid or incompatible store marker") from exc
    return True


def _empty(root: Path) -> bool:
    for entry in root.iterdir():
        if entry.name in (LOCK_NAME, PENDING_NAME):
            _regular(entry)
        else:
            return False
    return True


def _root_identity(root: Path) -> tuple[int, int]:
    try:
        info = root.stat()
        if not stat.S_ISDIR(info.st_mode):
            raise _incompatible("selected root is not a directory")
        return _identity(info)
    except OSError as exc:
        raise _incompatible("selected root is unavailable") from exc


def _wait(fd: int, mode: int, deadline: float) -> None:
    while True:
        try:
            fcntl.flock(fd, mode | fcntl.LOCK_NB)
            return
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StateBusy("State root is busy; retry after active engine operations finish") from exc
            time.sleep(min(0.01, remaining))



def _initialize_shared(fd: int, root: Path, deadline: float) -> None:
    """Initialize exclusively or join a peer's published store under SH.

    Drop prevalidation SH explicitly. A competing initializer may publish and
    retain SH for its process lifetime, so waiting only for EX would starve a
    compatible starter. Each failed EX attempt instead checks for publication
    under SH, then relinquishes SH again if initialization is still needed.
    """
    fcntl.flock(fd, fcntl.LOCK_UN)
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        else:
            if not _marker(root):
                if not _empty(root):
                    raise _incompatible("unmarked root is nonempty")
                _publish(root)
            _wait(fd, fcntl.LOCK_SH, deadline)
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
        else:
            if _marker(root):
                return
            fcntl.flock(fd, fcntl.LOCK_UN)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise StateBusy("State root is busy; retry after active engine operations finish")
        time.sleep(min(0.01, remaining))


def _publish(root: Path) -> None:
    pending = root / PENDING_NAME
    # Only the reserved, regular crash artifact may be discarded.
    if _regular(pending) is not None:
        pending.unlink()
    fd = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    try:
        payload = json.dumps({"product": PRODUCT, "format": "harflow.store", "v": 1,
                              "product_version": PRODUCT_VERSION, "layout_version": 1}, sort_keys=True).encode() + b"\n"
        with os.fdopen(fd, "wb", closefd=False) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(pending, root / MARKER_NAME)
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


@dataclass
class _Handle:
    fd: int
    root_id: tuple[int, int]
    lock_id: tuple[int, int]
    generation: int
    pid: int
    exclusive: bool
    refs: int = 0


_mutex = threading.RLock()
_handles: dict[tuple[int, int], _Handle] = {}
_generation = 0
_current: ContextVar[StateLease | None] = ContextVar("harflow_state_lease", default=None)
_jobs: set[asyncio.Task[Any]] = set()


class StateLease:
    """One independently closable reference to a live root lock."""

    def __init__(self, layout: StateLayout, handle: _Handle):
        self.layout = layout
        self._handle = handle
        self._closed = False
        self._contexts: list[Any] = []
        handle.refs += 1

    def _validate(self, layout: StateLayout) -> None:
        handle = self._handle
        if (self._closed or handle.refs <= 0 or handle.pid != os.getpid()
                or handle.generation != _generation
                or _handles.get(handle.root_id) is not handle
                or self.layout != layout):
            raise _incompatible("state capability is no longer live or belongs to another layout")
        try:
            if (_root_identity(layout.root) != handle.root_id
                    or _regular(layout.root / LOCK_NAME) is None
                    or _identity((layout.root / LOCK_NAME).lstat()) != handle.lock_id
                    or _identity(os.fstat(handle.fd)) != handle.lock_id):
                raise _incompatible("state root or ownership lock was replaced")
        except OSError as exc:
            raise _incompatible("ownership filesystem identity is unavailable") from exc

    def validate(self) -> None:
        """Validate this independent resource capability without changing its context."""
        with _mutex:
            self._validate(self.layout)

    def retain(self) -> StateLease:
        with _mutex:
            self._validate(self.layout)
            return StateLease(self.layout, self._handle)

    def close(self) -> None:
        with _mutex:
            if self._closed:
                return
            self._closed = True
            handle = self._handle
            # Fork invalidates before closing inherited descriptors. Never close a
            # stale numeric fd, which may already have been reused in the child.
            if handle.pid != os.getpid() or handle.generation != _generation:
                return
            handle.refs -= 1
            if handle.refs == 0:
                _handles.pop(handle.root_id, None)
                os.close(handle.fd)

    def __enter__(self) -> StateLease:
        with _mutex:
            self._validate(self.layout)
            self._contexts.append(_current.set(self))
        return self

    def __exit__(self, *exc: Any) -> None:
        try:
            _current.reset(self._contexts.pop())
        finally:
            self.close()


def current_owned() -> StateLease:
    """Return only a validated live capability from the current explicit context."""
    with _mutex:
        lease = _current.get()
        if lease is None:
            raise _incompatible("state access requires an active ownership session")
        lease._validate(lease.layout)
        return lease


def assert_owned(layout: StateLayout) -> StateLease:
    """Assert the exact selected layout has an active capability in this context."""
    with _mutex:
        lease = _current.get()
        if lease is None:
            raise _incompatible("state access requires an active ownership session")
        _validate_layout(layout)
        try:
            lease._validate(layout)
        except OSError as exc:
            raise _incompatible("ownership filesystem identity is unavailable") from exc
        return lease


@contextmanager
def _manager_until(deadline: float) -> Iterator[None]:
    if not _mutex.acquire(timeout=max(0, deadline - time.monotonic())):
        raise StateBusy("State ownership manager is busy; retry the operation")
    try:
        yield
    finally:
        _mutex.release()


def _validate_layout(layout: StateLayout) -> None:
    if (not isinstance(layout, StateLayout)
            or not isinstance(layout.root, Path)
            or not isinstance(layout.tasks_directory, Path)
            or str(layout.root) != os.path.abspath(layout.root)
            or str(layout.tasks_directory) != os.path.abspath(layout.tasks_directory)
            or layout.tasks_directory.parent != layout.root):
        raise _incompatible("selected layout paths are not normalized and coherent")


def _acquire(layout: StateLayout, timeout: float, exclusive: bool) -> StateLease:
    _validate_layout(layout)
    if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout must be finite and nonnegative")
    deadline = time.monotonic() + timeout
    root = layout.root
    # The same mutex protects all provisional descriptors as well as the cache,
    # so fork never inherits an uncatalogued ownership descriptor.
    with _manager_until(deadline):
        active = _current.get()
        if active is not None and not active._closed and active.layout.root == root:
            active._validate(active.layout)
        try:
            root_id = _root_identity(root)
        except StateIncompatible:
            if root.exists():
                raise
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            root_id = _root_identity(root)
        cached = _handles.get(root_id)
        if cached is not None:
            if exclusive:
                raise StateBusy("Cannot upgrade a live process state lease to exclusive ownership")
            lease = StateLease(layout, cached)
            try:
                lease._validate(layout)
                if not _marker(root):
                    raise _incompatible("store marker is missing")
                _regular(root / PENDING_NAME)
                return lease
            except BaseException:
                lease.close()
                raise
        lock = root / LOCK_NAME
        lock_info = _regular(lock)
        if lock_info is None:
            marked = _marker(root)
            empty = _empty(root) if not marked else False
            # A peer may have created the stable lock since the first stat.
            lock_info = _regular(lock)
            if lock_info is None and (marked or not empty):
                raise _incompatible("marked root has no stable lock" if marked else "unmarked root is nonempty")
        fd = -1
        try:
            if lock_info is None:
                try:
                    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
                except FileExistsError:
                    pass
            if fd < 0:
                fd = os.open(lock, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode):
                raise _incompatible("ownership lock is not regular")
            lock_id = _identity(info)
            if _root_identity(root) != root_id or _identity(lock.lstat()) != lock_id:
                raise _incompatible("root or lock changed while opening")
            _wait(fd, fcntl.LOCK_SH, deadline)
            _regular(root / PENDING_NAME)
            marked = _marker(root)
            if exclusive:
                fcntl.flock(fd, fcntl.LOCK_UN)
                _wait(fd, fcntl.LOCK_EX, deadline)
                if not _marker(root):
                    if not _empty(root):
                        raise _incompatible("unmarked root is nonempty")
                    _publish(root)
            elif not marked:
                _initialize_shared(fd, root, deadline)
            # Conversion may release the old lock; validate all identities again.
            if not _marker(root):
                raise _incompatible("store marker disappeared")
            handle = _Handle(fd, root_id, lock_id, _generation, os.getpid(), exclusive)
            _handles[root_id] = handle
            lease = StateLease(layout, handle)
            try:
                lease._validate(layout)
            except BaseException:
                lease.close()
                fd = -1
                raise
            fd = -1
            return lease
        finally:
            if fd >= 0:
                os.close(fd)


@contextmanager
def state_session(layout: StateLayout, *, timeout: float = DEFAULT_TIMEOUT) -> Iterator[StateLease]:
    """Hold validated shared ownership for a complete operation or process."""
    try:
        lease = _acquire(layout, timeout, False)
    except OSError as exc:
        raise _incompatible("ownership infrastructure is unavailable") from exc
    with lease:
        yield lease


@contextmanager
def exclusive_session(layout: StateLayout, *, timeout: float = DEFAULT_TIMEOUT) -> Iterator[StateLease]:
    """Narrow exclusive primitive; it does not authorize a store transformation."""
    try:
        lease = _acquire(layout, timeout, True)
    except OSError as exc:
        raise _incompatible("ownership infrastructure is unavailable") from exc
    with lease:
        yield lease


def _before_fork() -> None:
    _mutex.acquire()


def _after_parent() -> None:
    _mutex.release()


def _after_child() -> None:
    global _mutex, _generation, _handles, _jobs
    inherited = list(_handles.values())
    # Invalidate every inherited token before any inherited fd is closed.
    _generation += 1
    _handles = {}
    _jobs = set()
    _current.set(None)
    _mutex = threading.RLock()
    for handle in inherited:
        os.close(handle.fd)  # Never LOCK_UN: parent shares the open description.


os.register_at_fork(before=_before_fork, after_in_parent=_after_parent, after_in_child=_after_child)


def owned_job(layout: StateLayout, coroutine: Coroutine[Any, Any, Any], *, name: str | None = None) -> asyncio.Task[Any]:
    """Retain before scheduling; completion callbacks own exactly-once cleanup."""
    try:
        lease = assert_owned(layout).retain()
    except BaseException:
        coroutine.close()
        raise

    async def run() -> Any:
        token = _current.set(lease)
        try:
            return await coroutine
        finally:
            _current.reset(token)

    wrapper = run()
    try:
        task = asyncio.create_task(wrapper, name=name)
    except BaseException:
        wrapper.close()
        coroutine.close()
        lease.close()
        raise
    _jobs.add(task)

    def done(completed: asyncio.Task[Any]) -> None:
        try:
            coroutine.close()
        finally:
            lease.close()
            _jobs.discard(completed)

    task.add_done_callback(done)
    return task


_T = TypeVar("_T")


async def run_owned_worker(layout: StateLayout, function: Callable[..., _T], *args: Any, **kwargs: Any) -> _T:
    """Acquire in the actual worker, whose lifetime outlasts a cancelled waiter."""
    def run() -> _T:
        inherited = _current.get()
        with _mutex:
            expired = inherited is not None and inherited._closed
        token = _current.set(None) if expired else None
        try:
            with state_session(layout):
                return function(*args, **kwargs)
        finally:
            if token is not None:
                _current.reset(token)
    return await asyncio.to_thread(run)
