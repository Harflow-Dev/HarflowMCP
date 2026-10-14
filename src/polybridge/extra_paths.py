"""Typed, identity-checked authorization for existing writable paths."""
from collections.abc import Mapping
from dataclasses import dataclass, asdict
from pathlib import Path
import stat
import os
import unicodedata
from typing import Literal


def has_extra_path_authority(record: object) -> bool:
    """Detect saved extra-path markers, including malformed authority, without I/O.

    Only an empty list and a None certificate establish absence. Enforcement may
    omit both markers in legacy records; malformed values must not unlock an
    ordinary interactive resume with ambient permissions.
    """
    def marked(paths: object, certificate: object) -> bool:
        return not isinstance(paths, list) or bool(paths) or certificate is not None

    get = record.get if isinstance(record, Mapping) else lambda key, default: getattr(record, key, default)
    if marked(get("extra_writable_paths", []), get("extra_path_certification", None)):
        return True
    enforcement = get("enforcement", None)
    if enforcement is None:
        return False
    if not isinstance(enforcement, Mapping):
        return True
    return marked(enforcement.get("extra_writable_paths", []), enforcement.get("extra_path_certification", None))


@dataclass(frozen=True)
class PathGrant:
    requested: str
    path: str
    kind: Literal['directory', 'file']
    device: int
    inode: int


def _path(value: str) -> Path:
    if not isinstance(value, str) or not value or not Path(value).is_absolute():
        raise ValueError('extra writable paths must be absolute strings')
    if any(unicodedata.category(c) == 'Cc' for c in value):
        raise ValueError('extra writable paths cannot contain control characters')
    try:
        if len(value.encode('utf-8')) > 4096:
            raise ValueError('extra writable path exceeds 4096 UTF-8 bytes')
    except UnicodeError as exc:
        raise ValueError('extra writable path must be valid UTF-8') from exc
    return Path(value)


def _grant(value: str) -> PathGrant:
    try:
        path = _path(value).resolve(strict=True)
        info = path.stat()
    except OSError as exc:
        raise ValueError('extra writable path must exist and be accessible') from exc
    except RuntimeError as exc:
        raise ValueError('extra writable path has a symlink loop') from exc
    if path == Path(path.anchor):
        raise ValueError('filesystem roots cannot be granted')
    if stat.S_ISDIR(info.st_mode):
        kind = 'directory'
    elif stat.S_ISREG(info.st_mode):
        kind = 'file'
    else:
        raise ValueError('extra writable path must be a directory or regular file')
    return PathGrant(value, str(path), kind, info.st_dev, info.st_ino)


def normalize_paths(value: list[str] | None) -> tuple[PathGrant, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or len(value) > 16:
        raise ValueError('extra writable paths must be a list of at most 16 paths')
    grants = tuple(_grant(item) for item in value)
    if sum(len(g.requested.encode('utf-8')) for g in grants) > 16384:
        raise ValueError('extra writable paths exceed 16384 UTF-8 bytes')
    if len({g.path for g in grants}) != len(grants):
        raise ValueError('duplicate canonical extra writable path')
    return grants


def serialize_paths(grants: tuple[PathGrant, ...]) -> list[dict]:
    return [asdict(grant) for grant in grants]


def restore_paths(records: list[dict]) -> tuple[PathGrant, ...]:
    if not isinstance(records, list) or len(records) > 16:
        raise ValueError('invalid stored extra writable paths')
    grants = []
    for record in records:
        if not isinstance(record, dict) or set(record) != {'requested', 'path', 'kind', 'device', 'inode'}:
            raise ValueError('invalid stored path grant shape')
        _path(record['requested'])
        path = _path(record['path'])
        if os.path.normpath(record['path']) != record['path'] or str(path) != record['path'] or path == Path(path.anchor) or record['kind'] not in ('directory', 'file'):
            raise ValueError('invalid stored path grant')
        if any(type(record[key]) is not int or record[key] < 0 for key in ('device', 'inode')):
            raise ValueError('invalid stored path identity')
        grants.append(PathGrant(**record))
    if len({g.path for g in grants}) != len(grants) or sum(len(g.requested.encode('utf-8')) for g in grants) > 16384:
        raise ValueError('invalid stored path grants bounds')
    return tuple(grants)


def revalidate_paths(grants: tuple[PathGrant, ...]) -> None:
    for grant in grants:
        if _grant(grant.requested) != grant:
            raise ValueError('extra writable path identity or target changed')


def check_child_paths(parent_grants: tuple[PathGrant, ...], child_grants: tuple[PathGrant, ...], parent_repo: str) -> None:
    revalidate_paths(parent_grants)
    revalidate_paths(child_grants)
    repo = Path(parent_repo).resolve(strict=True)
    if not repo.is_dir():
        raise ValueError('owner repository must be a directory')
    for child in child_grants:
        target = Path(child.path)
        if target.is_relative_to(repo):
            continue
        if any((parent.kind == 'directory' and target.is_relative_to(Path(parent.path))) or
               (parent.kind == 'file' and child.kind == 'file' and child.path == parent.path)
               for parent in parent_grants):
            continue
        raise ValueError('child extra writable paths widen owner authorization')
