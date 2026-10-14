"""Backend-owned declarations of execution configuration namespaces.

These helpers normalize known inputs; they do not parse arbitrary hook graphs.
"""
import os
from pathlib import Path


def environment_path(name: str, default: Path | None = None) -> Path | None:
    value = os.environ.get(name)
    if value is None:
        if default is not None and not default.is_absolute():
            raise ValueError("backend configuration path environment is empty or relative")
        return default
    if not value:
        raise ValueError('backend configuration path environment is empty or relative')
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError('backend configuration path environment is empty or relative')
    return path


def project_namespaces(repo: Path, names: tuple[str, ...]) -> tuple[Path, ...]:
    return tuple(parent / name for parent in (repo, *repo.parents) for name in names)
