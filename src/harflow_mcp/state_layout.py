"""Pure selection and propagation of Harflow's engine state layout."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

STATE_ROOT_ENV = "HARFLOW_STATE_ROOT"


class InvalidStateRoot(ValueError):
    """The selected state root cannot describe a supported filesystem path."""


class UnsupportedStateLayout(ValueError):
    """An injected task directory cannot be reconstructed by a fresh process."""

    harflow_not_started = True


@dataclass(frozen=True)
class StateLayout:
    root: Path
    tasks_directory: Path


def _normalize(value: str | os.PathLike[str], *, override: bool = False) -> Path:
    try:
        raw = os.fspath(value)
        if not isinstance(raw, str) or not raw or "\0" in raw:
            raise ValueError
        if override and not os.path.isabs(raw):
            raise ValueError
        normalized = os.path.abspath(raw)
        if os.name == "posix":
            normalized = "/" + normalized.lstrip("/")
        return Path(normalized)
    except (TypeError, ValueError, OSError) as exc:
        detail = "HARFLOW_STATE_ROOT must be a nonempty absolute path without NUL characters" if override else "state paths must be valid nonempty filesystem paths without NUL characters"
        raise InvalidStateRoot("Invalid state root: " + detail) from exc


def default_home(environ: Mapping[str, str] | None = None) -> Path:
    """Use Path.home normally; explicit mappings describe their own effective HOME."""
    if environ is None:
        return Path.home()
    if os.name == "posix":
        home = environ.get("HOME")
        if home is None:
            import pwd
            home = pwd.getpwuid(os.getuid()).pw_dir
        return Path(home.rstrip("/") or "/")
    profile = environ.get("USERPROFILE")
    if profile is None:
        profile = environ.get("HOMEDRIVE", "") + environ.get("HOMEPATH", "")
    return Path(profile) if profile else Path.home()


def resolve_layout(*, root: str | os.PathLike[str] | None = None, log_dir: str | os.PathLike[str] | None = None, environ: Mapping[str, str] | None = None) -> StateLayout:
    """Explicit root/task directory wins, then the override, then the default home seam.

    Relative explicit paths and HOME values are normalized against the current directory.
    An explicit environment mapping never borrows HOME from the process environment.
    No symlink resolution or filesystem creation occurs.
    """
    if root is not None and log_dir is not None:
        raise InvalidStateRoot("Invalid state root: specify root or log_dir, not both")
    if log_dir is not None:
        tasks = _normalize(log_dir)
        return StateLayout(tasks.parent, tasks)
    if root is not None:
        selected = _normalize(root)
    else:
        env = os.environ if environ is None else environ
        if STATE_ROOT_ENV in env:
            selected = _normalize(env[STATE_ROOT_ENV], override=True)
        else:
            selected = _normalize(default_home(environ) / ".harflow")
    return StateLayout(selected, selected / "tasks")


def require_canonical_layout(layout: StateLayout) -> None:
    if (layout.root != _normalize(layout.root)
            or layout.tasks_directory != _normalize(layout.tasks_directory)
            or layout.tasks_directory.parent != layout.root
            or layout.tasks_directory.name != "tasks"):
        raise UnsupportedStateLayout("Unsupported state layout: fresh agent and supervisor launches require a task directory named exactly 'tasks' under the selected state root")


def require_dispatch_layout(log_dir: str | os.PathLike[str]) -> StateLayout:
    layout = resolve_layout(log_dir=log_dir)
    require_canonical_layout(layout)
    return layout


def child_environment(layout: StateLayout, environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Propagate the already-selected root, retaining independent vendor HOME discovery."""
    require_canonical_layout(layout)
    env = dict(os.environ if environ is None else environ)
    env[STATE_ROOT_ENV] = str(layout.root)
    return env


def protected_roots(*, log_dir: str | os.PathLike[str] | None = None, environ: Mapping[str, str] | None = None) -> frozenset[Path]:
    """Protect selected, default, legacy and independently valid ambient state roots."""
    env = os.environ if environ is None else environ
    home = default_home(environ)
    roots = {_normalize(home / ".harflow"), _normalize(home / ".polybridge")}
    roots.add(resolve_layout(log_dir=log_dir, environ=environ).root)
    if STATE_ROOT_ENV in env:
        try:
            roots.add(_normalize(env[STATE_ROOT_ENV], override=True))
        except InvalidStateRoot:
            pass
    return frozenset(roots)
