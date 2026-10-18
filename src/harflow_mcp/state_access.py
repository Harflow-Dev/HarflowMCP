"""Explicit state-operation scopes and resource-bound ownership capabilities."""
from __future__ import annotations

from functools import wraps
import inspect
import os
from pathlib import Path
import sqlite3
import threading
from typing import Any

from .state_layout import StateLayout, resolve_layout
from .state_ownership import StateIncompatible, StateOwnershipError, assert_owned, state_session, current_owned


def task_layout(directory: Path) -> StateLayout:
    """Resolve an explicitly identified task directory, including embedding layouts."""
    return resolve_layout(log_dir=directory)


def assert_task_owned(directory: Path):
    return assert_owned(task_layout(directory))


class _ScopedIterator:
    """Resume an iterator with a fresh capability, restoring context before yield."""

    def __init__(self, iterator, lifetime):
        self.iterator, self.lifetime = iterator, lifetime

    def __iter__(self):
        return self

    def _resume(self, function, *args):
        scope = self.lifetime.retain()
        try:
            with scope:
                return function(*args)
        finally:
            scope.close()

    def __next__(self):
        return self._resume(next, self.iterator)

    def send(self, value):
        return self._resume(self.iterator.send, value)

    def throw(self, *args):
        return self._resume(self.iterator.throw, *args)

    def close(self):
        invoked = False
        def close_inner():
            nonlocal invoked
            invoked = True
            return self.iterator.close()
        try:
            return self._resume(close_inner)
        except StateOwnershipError:
            if not invoked:
                try:
                    self.iterator.close()
                except BaseException:
                    pass
            raise


def _operation(function, select_layout):
    if inspect.isasyncgenfunction(function):
        raise TypeError("Asynchronous generator operation wrappers are unsupported")
    if inspect.iscoroutinefunction(function):
        @wraps(function)
        async def wrapped(*args, **kwargs):
            with state_session(select_layout(args, kwargs)):
                return await function(*args, **kwargs)
    elif inspect.isgeneratorfunction(function):
        @wraps(function)
        def wrapped(*args, **kwargs):
            with state_session(select_layout(args, kwargs)) as operation:
                lifetime = operation.retain()
            iterator = _ScopedIterator(function(*args, **kwargs), lifetime)
            try:
                try:
                    yield from iterator
                except BaseException:
                    try:
                        iterator.close()
                    except BaseException:
                        pass
                    raise
                else:
                    iterator.close()
            finally:
                lifetime.close()
    else:
        @wraps(function)
        def wrapped(*args, **kwargs):
            with state_session(select_layout(args, kwargs)):
                return function(*args, **kwargs)
    return wrapped


def task_operation(function=None, *, parameter=None):
    """Scope an operation's explicitly identified task-directory argument."""
    if function is None:
        return lambda selected: task_operation(selected, parameter=parameter)
    signature = inspect.signature(function)
    selected = parameter or next(iter(signature.parameters))
    return _operation(function, lambda args, kwargs: task_layout(signature.bind(*args, **kwargs).arguments[selected]))


def root_operation(function):
    """Scope an operation's explicit root parameter, preserving normal selection."""
    signature = inspect.signature(function)
    def select(args, kwargs):
        bound = signature.bind(*args, **kwargs)
        bound.apply_defaults()
        return resolve_layout(root=bound.arguments["root"])
    return _operation(function, select)


def storage_operation(function):
    """Scope a function whose first parameter is an explicit storage owner."""
    signature = inspect.signature(function)
    parameter = next(iter(signature.parameters))
    return _operation(function, lambda args, kwargs: signature.bind(*args, **kwargs).arguments[parameter].state_layout)


def path_operation(function):
    """Scope known state-path I/O using explicit layout or a live context only."""
    signature = inspect.signature(function)
    def select(args, kwargs):
        selected = signature.bind(*args, **kwargs).arguments.get("layout")
        return selected if selected is not None else current_owned().layout
    return _operation(function, select)


def layout_operation(function):
    """Scope one complete method using its owner's explicit state_layout value."""
    return _operation(function, lambda args, kwargs: args[0].state_layout)


def require_state_path(layout: StateLayout, path: Path):
    """Assert capability and lexical membership without deriving a root from a path."""
    lease = assert_owned(layout)
    selected = Path(os.path.abspath(path))
    if not selected.is_relative_to(layout.root):
        raise StateIncompatible("State path is outside the explicitly selected root")
    return lease


_resource_mutex = threading.RLock()
_descriptors: dict[int, OwnedDescriptor] = {}


class OwnedDescriptor(int):
    """An fd with an independent root reference until explicit release."""

    def __new__(cls, descriptor: int, lease):
        value = super().__new__(cls, descriptor)
        value.lease = lease
        value.pid = os.getpid()
        value.closed = False
        return value

    def close(self, *, unlock=False):
        with _resource_mutex:
            if self.closed:
                return
            self.closed = True
            if self.pid != os.getpid():
                self.lease.close()
                return
            _descriptors.pop(int(self), None)
            try:
                if unlock:
                    import fcntl
                    fcntl.flock(self, fcntl.LOCK_UN)
            finally:
                try:
                    os.close(self)
                finally:
                    self.lease.close()


def open_descriptor(path: Path, flags: int, mode: int = 0o600, *, layout: StateLayout, dir_fd: int | None = None) -> OwnedDescriptor:
    """Serialize open/register/close against fork; never inherit an uncached fd."""
    with _resource_mutex:
        retained = require_state_path(layout, path).retain()
        try:
            if dir_fd is None:
                descriptor = os.open(path, flags | os.O_CLOEXEC, mode)
            else:
                parent, opened = Path(path).parent.stat(), os.fstat(dir_fd)
                if (parent.st_dev, parent.st_ino) != (opened.st_dev, opened.st_ino):
                    raise StateIncompatible("State directory descriptor identity changed")
                descriptor = os.open(Path(path).name, flags | os.O_CLOEXEC, mode, dir_fd=dir_fd)
        except BaseException:
            retained.close()
            raise
        owned = OwnedDescriptor(descriptor, retained)
        _descriptors[descriptor] = owned
        return owned


def _resource_before_fork():
    _resource_mutex.acquire()


def _resource_after_parent():
    _resource_mutex.release()


def _resource_after_child():
    global _resource_mutex, _descriptors
    inherited = list(_descriptors.values())
    # Invalidate all wrappers before any descriptor closes, preventing a stale
    # wrapper from later closing an unrelated child descriptor with the same fd.
    _descriptors = {}
    _resource_mutex = threading.RLock()
    for resource in inherited:
        resource.closed = True
        resource.pid = -1
    for resource in inherited:
        os.close(resource)  # No LOCK_UN: the parent owns the shared description.


os.register_at_fork(before=_resource_before_fork, after_in_parent=_resource_after_parent,
                    after_in_child=_resource_after_child)


class OwnedCursor(sqlite3.Cursor):
    def _assert(self):
        self.connection._assert()

    def execute(self, *args, **kwargs):
        self._assert()
        return super().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        self._assert()
        return super().executemany(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        self._assert()
        return super().executescript(*args, **kwargs)

    def fetchone(self):
        self._assert()
        return super().fetchone()

    def fetchmany(self, *args):
        self._assert()
        return super().fetchmany(*args)

    def fetchall(self):
        self._assert()
        return super().fetchall()

    def __next__(self):
        self._assert()
        return super().__next__()


class OwnedConnection(sqlite3.Connection):
    """A SQLite connection whose actual lifetime retains root ownership."""

    _state_lease = None
    _state_pid = None

    def _assert(self):
        if self._state_lease is None:
            raise StateIncompatible("State database has no live ownership capability")
        self._state_lease.validate()

    def cursor(self, *args, **kwargs):
        self._assert()
        kwargs.setdefault('factory', OwnedCursor)
        return super().cursor(*args, **kwargs)

    def execute(self, *args, **kwargs):
        return self.cursor().execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        return self.cursor().executemany(*args, **kwargs)

    def executescript(self, *args, **kwargs):
        return self.cursor().executescript(*args, **kwargs)

    def commit(self):
        self._assert()
        return super().commit()

    def rollback(self):
        self._assert()
        return super().rollback()

    def __enter__(self):
        self._assert()
        return super().__enter__()

    def __exit__(self, *args):
        try:
            self._assert()
            return super().__exit__(*args)
        finally:
            self.close()

    def close(self):
        if self._state_pid is not None and self._state_pid != os.getpid():
            return  # Child cleanup must not enter SQLite or unlock parent resources.
        try:
            super().close()
        finally:
            lease, self._state_lease = self._state_lease, None
            if lease is not None:
                lease.close()


def bind_connection(connection: OwnedConnection, layout: StateLayout):
    connection._state_lease = assert_owned(layout).retain()
    connection._state_pid = os.getpid()
    return connection
