"""Bounded metadata-only references owned by verified task processes, standalone or workflow-bound."""
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
from . import control, identity, lineage, store
from .canonical_paths import canonical
from .extra_paths import restore_paths, revalidate_paths
from .catalog import METADATA_BYTES, catalog_lock, current_metadata_budget, metadata_request
from .bounded_io import ReadLimit

MAX_BYTES = 1048576
KINDS = frozenset(('screenshot', 'log', 'report', 'other'))
BOUND_ROLES = frozenset(('node', 'orchestrator', 'native_control'))
LIVE_ATTEMPTS = frozenset(('reserved', 'running', 'uncertain'))
TREE_LIMIT = 64
PAGE_BYTES = 63000


def _read_owner(log_dir, task_id):
    return store.read(log_dir, task_id, include_prompt=False,
                      metadata_byte_limit=METADATA_BYTES,
                      metadata_budget=current_metadata_budget())


def _owner_record_missing(log_dir, task_id):
    """Only observed absence establishes removal; unreadable/corrupt records do not."""
    try:
        store.record_path(log_dir, task_id).lstat()
    except FileNotFoundError:
        return True
    except OSError:
        return False
    return False


def _now():
    return datetime.now(timezone.utc).isoformat()


def _text(value, label):
    if not isinstance(value, str) or len(value.encode('utf-8')) > 4096 or '\0' in value:
        raise ValueError(f'{label} must be a string of at most 4096 UTF-8 bytes')
    return value


def _workflow_owner(log_dir, task_id, *, strict=True):
    """Exclude unverifiable ownership without scanning run history.

    A task's own receipt is returned as ``{'receipt': ...}`` so a caller can bind to it; a catalog
    association without a receipt is ``{'managed_or_unverifiable': True}``.
    """
    import sqlite3
    from .bounded_io import read_receipt
    from .workflows import WorkflowStore
    storage = WorkflowStore(root=log_dir.parent)
    receipt = storage.owners / f'{store.validate_task_id(task_id)}.json'
    try:
        receipt.lstat()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise ValueError('Workflow ownership is unavailable') from exc
    else:
        try:
            return {'receipt': read_receipt(receipt)}
        except (OSError, ValueError):
            # An unreadable receipt proves ownership exists but cannot be verified.
            return {'managed_or_unverifiable': True}
    try:
        page = storage.list_run_page(limit=1)
        if page.get('bootstrap_pending') or page.get('history_incomplete'):
            raise ValueError('Workflow ownership preparation is incomplete; retry the request')
        index = storage._ownership_catalog()
        with index.connect() as db:
            index._discover(db)
            if index.state(db)['status'] != 'ready':
                raise ValueError('Workflow ownership is unavailable')
            row = db.execute('SELECT 1 FROM associations WHERE id=? LIMIT 1', (task_id,)).fetchone()
        return {'managed_or_unverifiable': True} if row else None
    except (OSError, sqlite3.Error, TypeError, KeyError) as exc:
        raise ValueError('Workflow ownership is unavailable') from exc


def _bind(log_dir, record, receipt, *, registering):
    """Snapshot the immutable workflow binding from the task's own receipt and its run."""
    from .workflows import WorkflowStore
    storage = WorkflowStore(root=log_dir.parent)
    mismatch = 'Workflow ownership receipt no longer matches its run'
    run_id = receipt.get('workflow_run_id')
    if not isinstance(run_id, str):
        raise ValueError(mismatch)
    try:
        run = storage.get_run(run_id, metadata_byte_limit=METADATA_BYTES, metadata_budget=current_metadata_budget())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError('Workflow ownership is unavailable') from exc
    if run.get('kind') == 'builder':
        raise ValueError('Managed workflow artifact access is not supported for builders')
    activation = next((a for a in run.get('activations', []) if a.get('id') == receipt.get('workflow_execution_id')
                       and any(t.get('task_id') == record.task_id for t in a.get('tasks', []))), None)
    if activation is None or activation.get('role') != receipt.get('workflow_role'):
        raise ValueError(mismatch)
    root = (run.get('parent_link') or {}).get('root_workflow_run_id') or run['workflow_run_id']
    if receipt.get('root_workflow_run_id') != root:
        raise ValueError(mismatch)
    role = activation['role']
    if role not in BOUND_ROLES:
        raise ValueError('Managed workflow artifact access is not supported for this role')
    attempt = next(t for t in activation['tasks'] if t.get('task_id') == record.task_id)
    if registering and attempt.get('status') not in LIVE_ATTEMPTS:
        raise ValueError('Workflow attempt is no longer running; artifact registration is refused')
    binding = {'root_workflow_run_id': root, 'workflow_run_id': run['workflow_run_id'], 'node_id': activation.get('node_id'),
               'execution_id': activation['id'], 'role': role, 'task_id': record.task_id,
               'attribution': 'native_transport' if role == 'native_control' else 'attempt',
               'provenance': 'workflow_receipt'}
    for key in ('visit_id', 'attempt_in_visit', 'retry_of_execution_id'):
        if key in activation:
            binding[key] = activation[key]
    if role == 'native_control':
        hosted = attempt.get('native_owner_execution_ids')
        if hosted is None:
            hosted = [attempt['native_owner_execution_id']] if attempt.get('native_owner_execution_id') else []
        binding['hosted_native_attempt_ids'] = list(hosted)[:32]
    return binding


def _authority(log_dir, record, *, registering=False):
    """Return the workflow binding of a verified task, None for a standalone one, or refuse.

    The caller's own matching receipt binds it. Ancestors may be managed only inside the same
    root tree; an inherited descendant, a receipt mismatch and a builder are refused.
    """
    pending, seen, binding = [(record, frozenset())], set(), None
    while pending:
        current, ancestors = pending.pop()
        if current.task_id in ancestors:
            raise ValueError('Task ancestry is cyclic')
        if current.task_id in seen:
            continue
        seen.add(current.task_id)
        if len(seen) > 100:
            raise ValueError('Task ancestry exceeds authority bounds')
        if current.workflow_builder:
            raise ValueError('Managed workflow artifact access is not supported')
        owner = _workflow_owner(log_dir, current.task_id, strict=True)
        if current is record:
            if owner is not None:
                if 'receipt' not in owner:
                    raise ValueError('Managed workflow artifact access is not supported: ownership is unverifiable')
                binding = _bind(log_dir, record, owner['receipt'], registering=registering)
        elif owner is not None:
            receipt = owner.get('receipt')
            if binding is None or receipt is None:
                raise ValueError('Managed workflow artifact access is not supported for inherited descendants')
            ancestor_binding = _bind(log_dir, current, receipt, registering=False)
            if ancestor_binding['root_workflow_run_id'] != binding['root_workflow_run_id']:
                raise ValueError('Task ancestry belongs to another workflow tree')
        for parent in {value for value in (current.spawned_by, current.parent_task_id, current.continuation_of_task_id) if value}:
            ancestor = _read_owner(log_dir, parent)
            if ancestor is None:
                raise ValueError('Task ancestry is unavailable')
            pending.append((ancestor, ancestors | {current.task_id}))
    return binding


@metadata_request()
def _caller(log_dir):
    # Unlike best-effort dispatch lineage, authorization rejects ambiguity and spoofed hints.
    table = lineage._default_process_table()
    if table is None or os.getpid() not in table:
        raise ValueError('Caller ancestry is unavailable')
    ordered = lineage.ancestors(os.getpid(), table)
    chain = set(ordered)
    sid = os.getsid(0)
    matches = []
    for record in lineage.related_catalog_candidates(log_dir, ordered, sid):
        if record.pid in chain or record.pgid == sid:
            verdict = identity.identity_check(lineage._candidate_identity(record))
            if verdict not in ('alive', 'dead'):
                raise ValueError('Caller identity is unavailable')
            if verdict == 'alive':
                matches.append(record)
    # Nested standalone dispatch has several ancestors: only the nearest verified owner wins.
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
    selected = matches[0]
    record = _read_owner(log_dir, selected.task_id)
    if (record is None or
            (record.pid, record.pgid, record.start_time, record.markers) !=
            (selected.pid, selected.pgid, selected.start_time, selected.markers) or
            record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None or
            identity.identity_check(lineage._candidate_identity(record)) != 'alive'):
        raise ValueError('Caller task identity changed or is unavailable')
    _authority(log_dir, record)
    return record


def _path(log_dir, task_id):
    store.validate_task_id(task_id)
    return log_dir / f'{task_id}.artifacts.json'


def _check_binding(binding, task_id):
    """Strict structural validation of a stored workflow binding."""
    required = {'root_workflow_run_id', 'workflow_run_id', 'node_id', 'execution_id', 'role', 'task_id', 'attribution', 'provenance'}
    optional = {'visit_id', 'attempt_in_visit', 'retry_of_execution_id', 'hosted_native_attempt_ids'}
    bad = ValueError('Artifact manifest workflow binding is unavailable')
    if not isinstance(binding, dict) or not required <= set(binding) <= required | optional:
        raise bad
    if binding['task_id'] != task_id or binding['provenance'] != 'workflow_receipt' or binding['role'] not in BOUND_ROLES:
        raise bad
    for key in ('root_workflow_run_id', 'workflow_run_id', 'node_id', 'execution_id'):
        _text(binding[key], key)
    for key in ('visit_id', 'retry_of_execution_id'):
        if key in binding and binding[key] is not None:
            _text(binding[key], key)
    if 'attempt_in_visit' in binding and type(binding['attempt_in_visit']) is not int:
        raise bad
    transport = binding['role'] == 'native_control'
    if binding['attribution'] != ('native_transport' if transport else 'attempt') or transport != ('hosted_native_attempt_ids' in binding):
        raise bad
    if transport:
        hosted = binding['hosted_native_attempt_ids']
        if not isinstance(hosted, list) or len(hosted) > 32:
            raise bad
        for value in hosted:
            _text(value, 'hosted_native_attempt_ids')


def _load(log_dir: Path, task_id: str, directory_fd: int | None = None):
    path = _path(log_dir, task_id)
    budget = current_metadata_budget()
    if budget is not None and budget.metadata_decodes >= 100:
        raise ReadLimit('Metadata decode request limit reached')
    available = MAX_BYTES if budget is None else min(MAX_BYTES, max(0, budget.metadata_limit - budget.metadata_bytes))
    if log_dir.is_symlink() or canonical(log_dir) != log_dir.absolute():
        raise ValueError('Artifact storage directory is unsafe')
    try:
        fd = os.open(path if directory_fd is None else path.name,
                     os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except FileNotFoundError:
        return [], b''
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > available:
            raise ValueError('Artifact manifest is unavailable')
        with os.fdopen(fd, 'rb', buffering=0, closefd=False) as source:
            raw = source.read(available)
        if budget is not None:
            budget.metadata_bytes += len(raw)
        if os.fstat(fd).st_size > available:
            raise ReadLimit('Artifact manifest exceeds bounded read budget')
        if budget is not None:
            budget.metadata_decodes += 1
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
            if not isinstance(item, dict) or set(item) - {'workflow'} != {'artifact_id', 'task_id', 'path', 'kind', 'note', 'registered_at', 'provenance', 'registered_observation'} or item.get('task_id') != task_id or item.get('kind') not in KINDS:
                raise ValueError('Artifact manifest is unavailable')
            if 'workflow' in item:
                _check_binding(item['workflow'], task_id)
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


@metadata_request()
def summary(log_dir: Path, task_id: str) -> dict:
    """Summarize supported standalone or workflow-bound metadata without caller or target-file probes.

    Ownership must be established before opening a manifest. Unsupported or unknown
    ownership offers no detail reference; a supported but corrupt manifest keeps one
    so the detail API can report its unavailability.
    """
    try:
        record = _read_owner(log_dir, task_id)
        if record is None:
            raise ValueError('Artifact owner is unavailable')
        _authority(log_dir, record)
    except Exception:
        return {'count': 0, 'state': 'unavailable'}
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
        # Release the catalog lock before waiting for the lifecycle flock.
        with catalog_lock(log_dir):
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


@metadata_request()
def register(log_dir: Path, path: str, kind: str, note: str = '') -> dict:
    """Append an immutable reference under the owner's existing task flock.

    A workflow-bound caller's item carries the immutable binding snapshotted from its receipt.
    """
    if kind not in KINDS:
        raise ValueError('Unknown artifact kind')
    _text(note, 'note')
    caller = _caller(log_dir)
    if caller is None:
        raise ValueError('Artifact registration requires a verified task caller')
    if log_dir.is_symlink() or canonical(log_dir) != log_dir.absolute():
        raise ValueError('Artifact storage directory is unsafe')
    lock = control.record_lock_path(log_dir, caller.task_id)
    if lock.is_symlink():
        raise ValueError('Task lock is unsafe')
    with _task_lock(log_dir, caller.task_id) as storage_fd:
        record = _read_owner(log_dir, caller.task_id)
        if record is not None and (record.pid, record.pgid, record.start_time) != (caller.pid, caller.pgid, caller.start_time):
            raise ValueError('Artifact owner process identity changed')
        if record is None or record.status in store.TERMINAL_RECORD_STATUSES or identity.identity_check(lineage._candidate_identity(record)) != 'alive':
            raise ValueError('Artifact owner is no longer running')
        current_caller = _caller(log_dir)
        if current_caller is None or current_caller.task_id != record.task_id:
            raise ValueError('Caller identity changed')
        binding = _authority(log_dir, record, registering=True)
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
        if binding is not None:
            item['workflow'] = binding
        raw = json.dumps({'v': 1, 'task_id': record.task_id, 'items': items + [item]}, ensure_ascii=False).encode('utf-8')
        if len(raw) > MAX_BYTES:
            raise ValueError('Task artifact manifest limit reached')
        name = f'{record.task_id}.artifacts-{uuid.uuid4().hex}.tmp'
        # Catalog discovery needs a stable directory snapshot, including sidecars.
        with catalog_lock(log_dir):
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


def _availability(log_dir, record, item):
    """Stat-only availability of one reference; never reads contents."""
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
    return state


@metadata_request()
def get(log_dir: Path, task_id: str, cursor: str | None = None, limit: int = 20) -> dict:
    """Read a bounded reference page and observe target metadata without reading contents."""
    store.validate_task_id(task_id)
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError('Artifact limit must be between 1 and 50')
    caller = _caller(log_dir)
    if caller is not None and caller.task_id != task_id:
        raise ValueError('Task callers may read only their own artifacts')
    record = _read_owner(log_dir, task_id)
    if record is None:
        raise ValueError('Unknown task')
    _authority(log_dir, record)
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
        state = _availability(log_dir, record, item)
        entry = item | {'availability': {'state': state, 'observed_at': _now()}}
        if len(json.dumps(result | {'items': result['items'] + [entry]}, ensure_ascii=True).encode()) > 63000:
            break
        result['items'].append(entry)
    end = offset + len(result['items'])
    result.update(count=len(items), has_more=end < len(items))
    if result['has_more']:
        result['next_cursor'] = base64.urlsafe_b64encode(json.dumps([task_id, digest, end]).encode()).decode()
    return result


def _page_cursor(root_id, position, owner_task_id, artifact_id):
    return base64.urlsafe_b64encode(json.dumps([root_id, position[0], position[1], position[2], owner_task_id, artifact_id],
                                               separators=(',', ':')).encode()).decode()


def _read_page_cursor(cursor, workflow_run_id):
    try:
        if not isinstance(cursor, str) or len(cursor) > 512:
            raise ValueError()
        root_id, run_id, activation_index, task_index, owner, last = json.loads(base64.urlsafe_b64decode(cursor))
        if root_id != workflow_run_id or not isinstance(run_id, str) or type(activation_index) is not int or type(task_index) is not int:
            raise ValueError()
        if min(activation_index, task_index) < 0 or owner is not None and not isinstance(owner, str) or last is not None and not isinstance(last, str):
            raise ValueError()
        if owner is None and (activation_index != 0 or task_index != 0 or last is not None):
            raise ValueError()
        return run_id, activation_index, task_index, owner, last
    except Exception as exc:
        raise ValueError('Invalid or stale workflow artifact cursor') from exc


def _unavailable_page(workflow_run_id, reason):
    return {'workflow_run_id': workflow_run_id, 'items': [], 'owners': [], 'next_cursor': None, 'has_more': False,
            'state': 'unavailable', 'reason': reason, 'retry': reason != 'tree_bound_exceeded'}


def _budget_exhausted():
    budget = current_metadata_budget()
    return budget is not None and (budget.metadata_decodes >= 100 or budget.metadata_bytes >= budget.metadata_limit)


@metadata_request()
def workflow_page(log_dir: Path, workflow_run_id: str, *, cursor: str | None = None, limit: int = 20,
                  execution_id: str | None = None, task_id: str | None = None) -> dict:
    """Page the references registered by every attempt in a run's tree, in a stable owner order.

    Authority is the caller's concern (see ``workflow_node_reads.workflow_artifacts_page``). Tree
    membership comes from the run catalog alone; run records and manifests are read lazily under
    one metadata budget, so an unreadable run or manifest becomes an owner entry rather than a
    failed page, and every page makes progress.
    """
    from .bounded_io import ReadLimit
    from .workflows import WorkflowStore
    if not isinstance(workflow_run_id, str) or not workflow_run_id or any(v is not None and (not isinstance(v, str) or not v) for v in (execution_id, task_id)):
        raise ValueError('workflow_run_id, execution_id and task_id must be nonempty strings')
    if type(limit) is not int or not 1 <= limit <= 50:
        raise ValueError('Artifact limit must be between 1 and 50')
    storage = WorkflowStore(root=log_dir.parent)
    tree = storage.tree_members(workflow_run_id, TREE_LIMIT + 1)
    if tree['state'] != 'ready':
        # Preparation is a separate bounded phase; discovery itself remains catalog-only.
        # Return unavailable for this request even if this slice finishes the index.
        storage.prepare_run_catalog()
        return _unavailable_page(workflow_run_id, 'catalog_preparing')
    members = tree['members']
    if not members:
        raise ValueError('Unknown workflow run')
    if len(members) > TREE_LIMIT:
        return _unavailable_page(workflow_run_id, 'tree_bound_exceeded')
    member_ids = {member['id'] for member in members}
    order = [member['id'] for member in members]  # created_at, then id
    start = ((order[0], 0, 0, None, None) if cursor is None else _read_page_cursor(cursor, workflow_run_id))
    if start[0] not in member_ids:
        raise ValueError('Invalid or stale workflow artifact cursor')

    items: list[dict] = []
    owners: list[dict] = []
    seen_tasks: set[str] = set()
    seen_artifacts: set[str] = set()
    state = {'cursor': None}

    def size(extra_item=None, extra_owner=None):
        body = {'workflow_run_id': workflow_run_id, 'items': items + ([extra_item] if extra_item else []),
                'owners': owners + ([extra_owner] if extra_owner else []), 'next_cursor': 'x' * 512,
                'has_more': True, 'state': 'available'}
        return len(json.dumps(body, ensure_ascii=True).encode())

    def owner_entry(task, run_id, activation, state_name):
        return {'task_id': task, 'workflow_run_id': run_id, 'execution_id': activation, 'state': state_name}

    def append_owner(entry, position, owner_id=None, after=None):
        if len(owners) >= 50 or size(extra_owner=entry) > PAGE_BYTES:
            state['cursor'] = _page_cursor(workflow_run_id, position, owner_id, after)
            return False
        owners.append(entry)
        return True

    def full():
        return len(items) >= limit or len(owners) >= 50

    def produced():
        return bool(items or owners)

    done = True
    first_run = True
    for run_id in order[order.index(start[0]):]:
        position = (run_id, 0, 0)
        try:
            run = storage.get_run(run_id, metadata_byte_limit=METADATA_BYTES, metadata_budget=current_metadata_budget())
        except (OSError, ValueError, KeyError, TypeError) as exc:
            if isinstance(exc, ReadLimit) and produced():
                state['cursor'] = _page_cursor(workflow_run_id, position, None, None)
                done = False
                break
            if full() or size(extra_owner=owner_entry(None, run_id, None, 'run_unavailable')) > PAGE_BYTES:
                state['cursor'] = _page_cursor(workflow_run_id, position, None, None)
                done = False
                break
            owners.append({'workflow_run_id': run_id, 'state': 'run_unavailable'})
            first_run = False
            continue
        for child in (a.get('invocation') or {} for a in run.get('activations', [])):
            child_id = child.get('child_workflow_run_id')
            if child_id and child_id not in member_ids:
                return _unavailable_page(workflow_run_id, 'catalog_stale')
        activations = run.get('activations', [])
        attempt_ids = {t.get('task_id') for a in activations if execution_id and a.get('id') == execution_id for t in a.get('tasks', [])}
        # Owner-less cursors restart a run after a budget boundary, not a task position.
        resume = start if cursor is not None and first_run and start[0] == run_id and start[3] is not None else None
        first_run = False
        if resume is not None and (resume[1] >= len(activations) or resume[2] >= len(activations[resume[1]].get('tasks', []))):
            raise ValueError('Invalid or stale workflow artifact cursor')
        for ai, activation in enumerate(activations):
            if resume is not None and ai < resume[1]:
                continue
            for ti, attempt in enumerate(activation.get('tasks', [])):
                if resume is not None and ai == resume[1] and ti < resume[2]:
                    continue
                owner_id = attempt.get('task_id')
                position = (run_id, ai, ti)
                resumed_here = resume is not None and (ai, ti) == (resume[1], resume[2]) and resume[3] is not None
                if resumed_here and owner_id != resume[3]:
                    raise ValueError('Invalid or stale workflow artifact cursor')
                if not isinstance(owner_id, str) or attempt.get('execution_kind') == 'native_subagent' or owner_id in seen_tasks:
                    continue
                if execution_id and activation.get('id') != execution_id:
                    hosted = {attempt.get('native_owner_execution_id'), *(attempt.get('native_owner_execution_ids') or [])}
                    if activation.get('role') != 'native_control' or not hosted & attempt_ids:
                        continue
                if task_id and owner_id != task_id:
                    continue
                if full() or produced() and _budget_exhausted():
                    state['cursor'] = _page_cursor(workflow_run_id, position, owner_id, None)
                    done = False
                    break
                after = resume[4] if resumed_here else None
                seen_tasks.add(owner_id)
                try:
                    manifest, _ = _load(log_dir, owner_id)
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    if isinstance(exc, ReadLimit) and produced():
                        state['cursor'] = _page_cursor(workflow_run_id, position, owner_id, after)
                        done = False
                        break
                    if not append_owner(owner_entry(owner_id, run_id, activation.get('id'), 'unavailable'), position, owner_id, after):
                        done = False
                        break
                    continue
                if not manifest:
                    if _owner_record_missing(log_dir, owner_id):
                        if not append_owner(owner_entry(owner_id, run_id, activation.get('id'), 'record_removed'), position, owner_id, after):
                            done = False
                            break
                    continue
                if after is not None:
                    ids = [item['artifact_id'] for item in manifest]
                    if after not in ids:
                        raise ValueError('Invalid or stale workflow artifact cursor')
                    manifest = manifest[ids.index(after) + 1:]
                record = None
                last_emitted = after
                for item in manifest:
                    binding = item.get('workflow') or {}
                    root_id = (run.get('parent_link') or {}).get('root_workflow_run_id') or run_id
                    if any(binding.get(key) != value for key, value in (
                        ('task_id', owner_id), ('workflow_run_id', run_id), ('root_workflow_run_id', root_id),
                        ('execution_id', activation.get('id')), ('node_id', activation.get('node_id')),
                        ('role', activation.get('role')))):
                        continue
                    if execution_id and activation.get('id') != execution_id and not set(binding.get('hosted_native_attempt_ids', [])) & attempt_ids:
                        continue
                    if item['artifact_id'] in seen_artifacts:
                        continue
                    if record is None:
                        try:
                            record = _read_owner(log_dir, owner_id)
                        except (OSError, ValueError, KeyError, TypeError):
                            if not append_owner(owner_entry(owner_id, run_id, activation.get('id'), 'unavailable'), position, owner_id, after):
                                done = False
                            break
                        if record is None:
                            owner_state = 'record_removed' if _owner_record_missing(log_dir, owner_id) else 'unavailable'
                            if not append_owner(owner_entry(owner_id, run_id, activation.get('id'), owner_state), position, owner_id, after):
                                done = False
                            break
                    entry = item | {'availability': {'state': _availability(log_dir, record, item), 'observed_at': _now()}}
                    if not items and size(extra_item=entry) > PAGE_BYTES:
                        # An individually oversized reference must never pin the cursor.
                        if not append_owner(owner_entry(owner_id, run_id, activation.get('id'), 'unavailable'), position, owner_id, item['artifact_id']):
                            done = False
                            break
                        last_emitted = item['artifact_id']
                        continue
                    if len(items) >= limit or size(extra_item=entry) > PAGE_BYTES:
                        state['cursor'] = _page_cursor(workflow_run_id, position, owner_id, last_emitted)
                        done = False
                        break
                    items.append(entry)
                    seen_artifacts.add(item['artifact_id'])
                    last_emitted = item['artifact_id']
                if not done:
                    break
            if not done:
                break
        if not done:
            break
    result = {'workflow_run_id': workflow_run_id, 'items': items, 'owners': owners, 'next_cursor': state['cursor'],
              'has_more': not done, 'state': 'available', 'count': len(items)}
    return result


@metadata_request()
def owner_references(log_dir: Path, owner_task_id: str) -> list[dict]:
    """Current references of one owner with availability; empty when it has none.

    Used by workflow delivery, which needs the same stat observation as the readers. A manifest or
    owner record that cannot be read raises ``ValueError`` so a caller never mistakes it for none.
    """
    try:
        items, _ = _load(log_dir, owner_task_id)
    except (OSError, KeyError, TypeError) as exc:
        raise ValueError('Artifact manifest is unavailable') from exc
    if not items:
        return []
    record = _read_owner(log_dir, owner_task_id)
    return [item | {'availability': {'state': 'unavailable' if record is None else _availability(log_dir, record, item), 'observed_at': _now()}} for item in items]
