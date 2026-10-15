"""Bounded metadata-only references owned by verified standalone task processes."""
from __future__ import annotations
import base64
from contextlib import contextmanager
import fcntl
import time
import hashlib
import json
import os
import stat
import uuid
from datetime import datetime, timezone
from pathlib import Path
from . import control, identity, lineage, store, workflow_hooks
from .canonical_paths import canonical
from .extra_paths import restore_paths, revalidate_paths

MAX_BYTES = 1048576
KINDS = frozenset(('screenshot', 'log', 'report', 'other'))


def _now():
    return datetime.now(timezone.utc).isoformat()


def _text(value, label):
    if not isinstance(value, str) or len(value.encode('utf-8')) > 4096 or '\0' in value:
        raise ValueError(f'{label} must be a string of at most 4096 UTF-8 bytes')
    return value


def _standalone(log_dir, record):
    pending, seen = [(record, frozenset())], set()
    while pending:
        record, ancestors = pending.pop()
        if record.task_id in ancestors:
            raise ValueError('Task ancestry is cyclic')
        if record.task_id in seen:
            continue
        seen.add(record.task_id)
        if len(seen) > 100:
            raise ValueError('Task ancestry exceeds authority bounds')
        if record.workflow_builder or workflow_hooks.owner(log_dir, record.task_id, strict=True) is not None:
            raise ValueError('Managed workflow artifact access is not supported')
        for parent in {value for value in (record.spawned_by, record.parent_task_id) if value}:
            ancestor = store.read(log_dir, parent, include_prompt=False)
            if ancestor is None:
                raise ValueError('Task ancestry is unavailable')
            pending.append((ancestor, ancestors | {record.task_id}))


def _caller(log_dir):
    # Unlike best-effort dispatch lineage, authorization rejects ambiguity and spoofed hints.
    table = lineage._default_process_table()
    if table is None or os.getpid() not in table:
        raise ValueError('Caller ancestry is unavailable')
    chain = set(lineage.ancestors(os.getpid(), table))
    sid = os.getsid(0)
    matches = []
    for record in lineage._candidates(log_dir):
        if record.pid in chain or record.pgid == sid:
            verdict = identity.identity_check(lineage._candidate_identity(record))
            if verdict not in ('alive', 'dead'):
                raise ValueError('Caller identity is unavailable')
            if verdict == 'alive':
                matches.append(record)
    # Nested standalone dispatch has several ancestors: only the nearest verified owner wins.
    ordered = lineage.ancestors(os.getpid(), table)
    ancestral = [record for record in matches if record.pid in chain]
    if ancestral:
        nearest = min(ordered.index(record.pid) for record in ancestral)
        selected = [record for record in ancestral if ordered.index(record.pid) == nearest]
        # A session-only rival cannot be disambiguated by ancestry.
        if any(record.pid not in chain for record in matches):
            raise ValueError('Caller task identity is ambiguous')
        matches = selected
    hint = os.environ.get('PB_TASK_ID')
    if len(matches) > 1 or (hint and (len(matches) != 1 or matches[0].task_id != hint)):
        raise ValueError('Caller task identity is ambiguous or unverifiable')
    if not matches:
        if os.environ.get('PB_ROOT_TASK_ID'):
            raise ValueError('Caller task identity is unverifiable')
        return None
    _standalone(log_dir, matches[0])
    return matches[0]


def _path(log_dir, task_id):
    store.validate_task_id(task_id)
    return log_dir / f'{task_id}.artifacts.json'


def _load(log_dir: Path, task_id: str, directory_fd: int | None = None):
    path = _path(log_dir, task_id)
    if log_dir.is_symlink() or canonical(log_dir) != log_dir.absolute():
        raise ValueError('Artifact storage directory is unsafe')
    try:
        fd = os.open(path if directory_fd is None else path.name,
                     os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except FileNotFoundError:
        return [], b''
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_BYTES:
            raise ValueError('Artifact manifest is unavailable')
        with os.fdopen(fd, 'rb', closefd=False) as source:
            raw = source.read(MAX_BYTES + 1)
        try:
            data = json.loads(raw)
        except RecursionError as exc:
            raise ValueError('Artifact manifest nesting is unavailable') from exc
        if len(raw) > MAX_BYTES or not isinstance(data, dict) or data.get('v') != 1 or data.get('task_id') != task_id:
            raise ValueError('Artifact manifest is unavailable')
        items = data['items']
        if not isinstance(items, list) or len(items) > 100:
            raise ValueError('Artifact manifest is unavailable')
        for item in items:
            if not isinstance(item, dict) or set(item) != {'artifact_id', 'task_id', 'path', 'kind', 'note', 'registered_at', 'provenance', 'registered_observation'} or item.get('task_id') != task_id or item.get('kind') not in KINDS:
                raise ValueError('Artifact manifest is unavailable')
            for key in ('path', 'note', 'artifact_id', 'registered_at'):
                _text(item[key], key)
            uuid.UUID(item['artifact_id'])
            if not Path(item['path']).is_absolute() or not isinstance(item['provenance'], dict):
                raise ValueError('Artifact manifest is unavailable')
            observation = item['registered_observation']
            if not isinstance(observation, dict) or set(observation) != {'device', 'inode', 'size', 'mtime_ns'} or any(type(v) is not int for v in observation.values()):
                raise ValueError('Artifact manifest is unavailable')
            if any(observation[key] < 0 for key in ('device', 'inode', 'size')):
                raise ValueError('Artifact manifest is unavailable')
        ids = [item['artifact_id'] for item in items]
        if len(set(ids)) != len(ids) or any(item['provenance'] != {'method': 'verified_process_lineage', 'task_id': task_id} for item in items):
            raise ValueError('Artifact manifest provenance is unavailable')
        return items, raw
    finally:
        os.close(fd)


def summary(log_dir: Path, task_id: str) -> dict:
    """Bounded manifest-only summary for task detail; never stats artifact targets."""
    try:
        items, _ = _load(log_dir, task_id)
        count, state = len(items), 'available'
    except (OSError, ValueError, KeyError, TypeError):
        count, state = 0, 'unavailable'
    return {'count': count, 'state': state, 'detail_ref': {'tool': 'get_task_artifacts', 'task_id': task_id}}


def _observation(info):
    return {'device': info.st_dev, 'inode': info.st_ino, 'size': info.st_size, 'mtime_ns': info.st_mtime_ns}


def _regular_stat(path):
    """Observe metadata without following any component replaced after canonicalization."""
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in path.parts[1:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        flags = getattr(os, 'O_EVTONLY', getattr(os, 'O_PATH', os.O_RDONLY))
        final = os.open(path.name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
        try:
            info = os.fstat(final)
            if not stat.S_ISREG(info.st_mode):
                raise ValueError('Artifact must be an existing regular file')
            return info
        finally:
            os.close(final)
    finally:
        os.close(descriptor)


def _target(log_dir, record, spelling):
    _text(spelling, 'path')
    recorded_repo = Path(record.repo_path)
    try:
        repo = canonical(recorded_repo)
    except FileNotFoundError:
        repo = None
    if not Path(spelling).is_absolute() and repo is None:
        raise ValueError('Recorded repository authority is unavailable')
    target = canonical(Path(spelling) if Path(spelling).is_absolute() else repo / spelling)
    from .scratch import directory
    scratch = directory(log_dir, record.task_id)
    own_scratch = not scratch.is_symlink() and not scratch.parent.is_symlink() and target.is_relative_to(scratch.resolve()) and str(scratch.resolve()) in (record.enforcement or {}).get('writable_roots', [])
    if not own_scratch:
        # Backend state and bridge state are never user artifact authority.
        forbidden = ('.polybridge', '.codex', '.claude', '.vibe', '.config', '.git')
        if target.is_relative_to(log_dir.parent.resolve()) or any(part in forbidden for part in target.parts):
            raise ValueError('Artifact is in protected bridge or backend state')
        from . import backends
        from .runtime_paths import _chain
        namespaces = []
        for backend in backends.BACKENDS.values():
            namespaces.extend(backend.artifact_configuration_namespaces(repo or recorded_repo))
        for namespace in namespaces:
            _, resolved = _chain(namespace)
            if target.is_relative_to(namespace) or target.is_relative_to(resolved):
                raise ValueError('Artifact is in protected backend configuration')
    if not (own_scratch or (repo is not None and target.is_relative_to(repo))):
        # Validate the entire saved shape, but authority is independent per grant.
        # A revoked unrelated grant (or narrower overlapping grant) cannot revoke
        # another still-valid grant that covers this canonical target.
        grants = restore_paths(record.extra_writable_paths)
        for grant in grants:
            if not ((grant.kind == 'directory' and target.is_relative_to(Path(grant.path))) or
                    (grant.kind == 'file' and str(target) == grant.path)):
                continue
            try:
                revalidate_paths((grant,))
            except (OSError, ValueError, RuntimeError):
                continue
            break
        else:
            raise ValueError('Artifact path has no valid covering task authority')
    _text(str(target), 'canonical path')
    info = _regular_stat(target)
    return target, _observation(info)


@contextmanager
def _task_lock(log_dir, task_id):
    """Use the existing task flock inode, refusing symlinks and special files."""
    directory_fd = os.open(log_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    fd = None
    try:
        lock_name = f'{task_id}.lock'
        flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
        try:
            # Darwin can return ENOENT when O_CREAT|O_NOFOLLOW races another initial
            # creator. Exclusive creation, then a no-create open of its inode, avoids it.
            fd = os.open(lock_name, flags | os.O_CREAT | os.O_EXCL, 0o644,
                         dir_fd=directory_fd)
        except FileExistsError:
            fd = os.open(lock_name, flags, dir_fd=directory_fd)
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ValueError('Task lock is unsafe')
        deadline = time.monotonic() + control.RECORD_LOCK_TIMEOUT_SECONDS
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise ValueError('Task artifact lock timed out') from None
                time.sleep(0.01)
        linked_lock = os.stat(lock_name, dir_fd=directory_fd, follow_symlinks=False)
        if (linked_lock.st_dev, linked_lock.st_ino) != (info.st_dev, info.st_ino):
            raise ValueError('Task lock identity changed')
        current = log_dir.stat()
        original = os.fstat(directory_fd)
        if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino) or log_dir.is_symlink():
            raise ValueError('Artifact storage directory changed')
        yield directory_fd
    finally:
        if fd is not None:
            os.close(fd)
        os.close(directory_fd)


def register(log_dir: Path, path: str, kind: str, note: str = '') -> dict:
    """Append an immutable reference under the owner's existing task flock."""
    if kind not in KINDS:
        raise ValueError('Unknown artifact kind')
    _text(note, 'note')
    caller = _caller(log_dir)
    if caller is None:
        raise ValueError('Artifact registration requires a verified standalone task caller')
    if log_dir.is_symlink() or canonical(log_dir) != log_dir.absolute():
        raise ValueError('Artifact storage directory is unsafe')
    lock = control.record_lock_path(log_dir, caller.task_id)
    if lock.is_symlink():
        raise ValueError('Task lock is unsafe')
    with _task_lock(log_dir, caller.task_id) as storage_fd:
        record = store.read(log_dir, caller.task_id, include_prompt=False)
        if record is not None and (record.pid, record.pgid, record.start_time) != (caller.pid, caller.pgid, caller.start_time):
            raise ValueError('Artifact owner process identity changed')
        if record is None or record.status in store.TERMINAL_RECORD_STATUSES or identity.identity_check(lineage._candidate_identity(record)) != 'alive':
            raise ValueError('Artifact owner is no longer running')
        current_caller = _caller(log_dir)
        if current_caller is None or current_caller.task_id != record.task_id:
            raise ValueError('Caller identity changed')
        _standalone(log_dir, record)
        target, observed = _target(log_dir, record, path)
        if not os.access(target, os.R_OK):
            raise ValueError('Artifact file is inaccessible')
        items, _ = _load(log_dir, record.task_id, storage_fd)
        for item in items:
            if (item['path'], item['kind'], item['note'], item['registered_observation']) == (str(target), kind, note, observed):
                return item
        if len(items) >= 100:
            raise ValueError('Task artifact limit reached')
        item = {'artifact_id': str(uuid.uuid4()), 'task_id': record.task_id, 'path': str(target), 'kind': kind, 'note': note, 'registered_at': _now(), 'provenance': {'method': 'verified_process_lineage', 'task_id': record.task_id}, 'registered_observation': observed}
        raw = json.dumps({'v': 1, 'task_id': record.task_id, 'items': items + [item]}, ensure_ascii=False).encode('utf-8')
        if len(raw) > MAX_BYTES:
            raise ValueError('Task artifact manifest limit reached')
        name = f'{record.task_id}.artifacts-{uuid.uuid4().hex}.tmp'
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=storage_fd)
        try:
            with os.fdopen(fd, 'wb') as output:
                output.write(raw)
                output.flush()
                os.fsync(output.fileno())
            os.replace(name, _path(log_dir, record.task_id).name,
                       src_dir_fd=storage_fd, dst_dir_fd=storage_fd)
            os.fsync(storage_fd)
        finally:
            try:
                os.unlink(name, dir_fd=storage_fd)
            except FileNotFoundError:
                pass
        return item


def get(log_dir: Path, task_id: str, cursor: str | None = None, limit: int = 20) -> dict:
    """Read a bounded reference page and observe target metadata without reading contents."""
    store.validate_task_id(task_id)
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError('Artifact limit must be between 1 and 50')
    caller = _caller(log_dir)
    if caller is not None and caller.task_id != task_id:
        raise ValueError('Task callers may read only their own artifacts')
    record = store.read(log_dir, task_id, include_prompt=False)
    if record is None:
        raise ValueError('Unknown task')
    _standalone(log_dir, record)
    result = {'task_id': task_id, 'items': [], 'next_cursor': None, 'has_more': False, 'state': 'available', 'count': 0}
    try:
        items, raw = _load(log_dir, task_id)
    except (OSError, ValueError, KeyError, TypeError):
        return result | {'state': 'unavailable'}
    digest = hashlib.sha256(raw).hexdigest()
    offset = 0
    if cursor is not None:
        try:
            if not isinstance(cursor, str) or len(cursor) > 512:
                raise ValueError()
            owner, saved, offset = json.loads(base64.urlsafe_b64decode(cursor))
            if owner != task_id or saved != digest or type(offset) is not int or not 0 <= offset <= len(items):
                raise ValueError()
        except Exception as exc:
            raise ValueError('Invalid or stale artifact cursor') from exc
    for item in items[offset:offset + limit]:
        state = 'unavailable'
        path = Path(item['path'])
        try:
            current_path = canonical(path)
        except FileNotFoundError:
            # Only absence of the artifact path itself establishes deletion. Missing
            # authority (repository or saved grant roots) is handled separately below.
            state = 'deleted'
        except (OSError, ValueError, RuntimeError):
            pass
        else:
            if current_path != path:
                state = 'changed'
            else:
                try:
                    _, observed = _target(log_dir, record, str(path))
                    if os.access(path, os.R_OK):
                        state = 'available' if observed == item['registered_observation'] else 'changed'
                except (OSError, ValueError, RuntimeError):
                    pass
        entry = item | {'availability': {'state': state, 'observed_at': _now()}}
        if len(json.dumps(result | {'items': result['items'] + [entry]}, ensure_ascii=True).encode()) > 63000:
            break
        result['items'].append(entry)
    end = offset + len(result['items'])
    result.update(count=len(items), has_more=end < len(items))
    if result['has_more']:
        result['next_cursor'] = base64.urlsafe_b64encode(json.dumps([task_id, digest, end]).encode()).decode()
    return result
