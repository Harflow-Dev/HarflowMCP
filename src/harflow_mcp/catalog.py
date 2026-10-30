"""Small durable listing index. Historical records are read, never migrated in place."""
from __future__ import annotations

from .state_layout import StateLayout
from .state_access import OwnedConnection, bind_connection, require_state_path, layout_operation, open_descriptor
from .state_ownership import state_session, assert_owned, current_owned, StateIncompatible

import base64
import hashlib
import fcntl
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
import json
import stat
import logging
import os
import math
import sqlite3
from pathlib import Path
from typing import Any, Callable

from .read_metrics import span

CATALOG_FILENAME = '.listing.v11.sqlite3'
TASK_PROJECTION_VERSION = '3'
RUN_PROJECTION_VERSION = '4'
# A filtered listing page examines at most this many catalog rows after its cursor.
SCAN_ROWS = 2000


def catalog_path(directory: Path) -> Path:
    """Keep independent compatible engines in separate derivative namespaces."""
    return directory / CATALOG_FILENAME


PAGE_LIMIT = 100
PAGE_BYTES = 256 * 1024
ROW_BYTES = 128 * 1024  # Reserve the other half for ownership/ancestor metadata.
BOOTSTRAP_LIMIT = 100
METADATA_BYTES = 4 * 1024 * 1024
METADATA_BATCH_BYTES = 8 * 1024 * 1024



@dataclass
class MetadataBudget:
    metadata_bytes: int = 0
    metadata_decodes: int = 0
    metadata_limit: int = METADATA_BATCH_BYTES
    authority_preparation: dict[str, Any] | None = None
    authority_directory: str | None = None


_request_budget: ContextVar[MetadataBudget | None] = ContextVar('catalog_request_budget', default=None)


def current_metadata_budget() -> MetadataBudget | None:
    """Return the current request's shared raw metadata read budget."""
    return _request_budget.get()


_unscoped_authority_preparation: ContextVar[tuple[str | None, dict[str, Any] | None] | None] = ContextVar('unscoped_authority_preparation', default=None)


def set_authority_preparation(state: dict[str, Any] | None, directory: Path | None = None) -> None:
    """Update the request object shared with to_thread, never a shared catalog row."""
    value = dict(state) if state is not None else None
    scope = str(directory.resolve()) if directory is not None else None
    budget = _request_budget.get()
    if budget is not None:
        budget.authority_preparation = value
        budget.authority_directory = scope
    else:
        _unscoped_authority_preparation.set((scope, value))


def get_authority_preparation(directory: Path | None = None) -> dict[str, Any] | None:
    budget = _request_budget.get()
    if budget is not None:
        scope, state = budget.authority_directory, budget.authority_preparation
    else:
        scope, state = _unscoped_authority_preparation.get() or (None, None)
    if directory is not None and scope != str(directory.resolve()):
        return None
    return dict(state) if state is not None else None


@contextmanager
def metadata_request():
    if _request_budget.get() is not None:
        yield
        return
    token = _request_budget.set(MetadataBudget())
    try:
        yield
    finally:
        _request_budget.reset(token)


def bounded_request(function):
    """Share the indexing budget through nested async readers and asyncio.to_thread."""
    @wraps(function)
    async def wrapped(*args, **kwargs):
        with metadata_request():
            return await function(*args, **kwargs)
    return wrapped


def source_identity(source):
    return (source.st_mtime_ns, source.st_size, getattr(source, 'st_ino', 0), getattr(source, 'st_dev', 0))


def merge_read_state(previous: dict[str, Any] | None, state: dict[str, Any]) -> dict[str, Any]:
    """Persistent failures win presentation; deferred work survives other sources."""
    if not previous or previous.get('status') == 'ready':
        return dict(state)
    chosen = previous if previous.get('status') == 'blocked' and state['status'] != 'blocked' else state
    result = dict(chosen)
    for key in ('pending_records', 'blocked_records'):
        result[key] = max(previous.get(key, 0), state.get(key, 0))
    blockers = sorted(set(previous.get('blocker_types', []) + state.get('blocker_types', [])))
    if blockers:
        result['blocker_types'] = blockers[:10]
    return result


def apply_read_state(page: dict[str, Any], state: dict[str, Any]) -> None:
    """Keep incomplete read projections neutral, with persistent blockers winning."""
    if state['status'] == 'ready':
        return
    previous = page.get('catalog_state', {})
    merged = merge_read_state(previous, state)
    page['catalog_state'] = merged
    pending = merged['status'] == 'preparing' or bool(merged.get('pending_records')) or bool(previous) and page.get('bootstrap_pending', False)
    page.update(bootstrap_pending=bool(pending), counts_complete=False,
                history_incomplete=page.get('history_incomplete', False) or merged['status'] == 'blocked')
    for key in ('total_active_count', 'total_active_root_count', 'total_attention_root_count'):
        if key in page:
            page[key] = None


class RelatedHeaders(list):
    """Compact ancestry plus the readiness of every requested source."""
    catalog_state: dict[str, Any] | None = None


class DeferredRead(Exception):
    """The request budget is exhausted; leave this source queued for the next read."""


_locks: dict[str, tuple[threading.RLock, threading.local]] = {}
_locks_guard = threading.Lock()


def _catalog_after_child():
    global _locks, _locks_guard
    _locks = {}
    _locks_guard = threading.Lock()


os.register_at_fork(after_in_child=_catalog_after_child)


@contextmanager
def catalog_lock(directory: Path, *, layout: StateLayout | None = None):
    """Stable process/thread lock; nested projections retain the writer's outer lock."""
    selected = layout if layout is not None else current_owned().layout
    # Resource ownership outlives acquisition without retaining a ContextVar
    # token across API returns (Catalog.connect returns this lock to close).
    with state_session(selected) as operation:
        require_state_path(selected, directory)
        directory.mkdir(parents=True, exist_ok=True)
        key = str(directory.resolve())
        with _locks_guard:
            mutex, local = _locks.setdefault(key, (threading.RLock(), threading.local()))
        retained = operation.retain()
    try:
        with mutex:
            retained.validate()
            depth = getattr(local, 'depth', 0)
            if not depth:
                # The independent root ref is already live; make it current only
                # for descriptor creation, never across the resource yield.
                with state_session(selected):
                    local.handle = open_descriptor(directory / '.listing.lock', os.O_RDWR | os.O_CREAT, 0o600, layout=selected)
                try:
                    fcntl.flock(local.handle, fcntl.LOCK_EX)
                except BaseException:
                    local.handle.close()
                    raise
            local.depth = depth + 1
            try:
                yield
            finally:
                local.depth -= 1
                if not local.depth:
                    local.handle.close(unlock=True)
    finally:
        retained.close()



class CatalogConnection(OwnedConnection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()

    def close(self):
        if self._state_pid is not None and self._state_pid != os.getpid():
            return  # Do not unwind the parent's inherited catalog lock.
        try:
            super().close()
        finally:
            lock = getattr(self, '_catalog_lock', None)
            if lock is not None:
                self._catalog_lock = None
                lock.__exit__(None, None, None)

def bound_header(header: dict[str, Any]) -> dict[str, Any]:
    """A header is at most 2 KiB; full content remains on the direct detail route."""
    result = dict(header)
    if isinstance(result.get('sessions'), dict):
        result['sessions'] = {'orchestrator': str(result['sessions']['orchestrator'])[:128]} if result['sessions'].get('orchestrator') else {}
    protected = {'task_id', 'workflow_run_id', 'parent_workflow_run_id', 'root_workflow_run_id', 'orchestrator_session_owner_run_id', 'workflow_session_owner_run_id', 'session_id', 'parent_task_id', 'continuation_of_task_id', 'chain_root_task_id', 'root_task_id', 'spawned_by', 'status', 'started_at', 'created_at', 'updated_at', 'backend', 'kind', 'execution_contract', 'needs_direct_lookup', 'sessions'}
    metadata = {'finished_at', 'resumed_at', 'workflow_id', 'workflow_builder', 'kind', 'settling', 'workflow_run_id', 'parent_workflow_run_id', 'root_workflow_run_id'}
    protected.update(metadata)
    protected.update({'persisted_status', 'observed_exit', 'process_identity_state', 'needs_reconciliation', 'status_reconciled'})
    for key, value in list(result.items()):
        if isinstance(value, str) and key not in metadata:
            result[key] = value[:128 if key in protected else 200]
    for maximum in (100, 32, 8):
        if len(json.dumps(result, ensure_ascii=True).encode()) <= 2048:
            return result
        for key, value in list(result.items()):
            if key not in protected:
                if isinstance(value, str):
                    result[key] = value[:maximum]
                elif isinstance(value, (dict, list)):
                    result.pop(key)
    if len(json.dumps(result, ensure_ascii=True).encode()) > 2048:
        result = {key: value for key, value in result.items() if key in protected}
    if len(json.dumps(result, ensure_ascii=True).encode()) > 2048:
        # Do not publish truncated classification or identity as complete evidence.
        key = 'task_id' if 'task_id' in header else 'workflow_run_id'
        result = {key: str(header.get(key, ''))[:100], 'status': 'unknown', 'needs_direct_lookup': True,
                  'index_blocker': 'header_metadata_budget'}
    return result


def _run_filter_conditions(filters: dict[str, Any]) -> tuple[list[str], list[Any]]:
    """SQL for normalized run filters over ``entries``; lossless columns come from ``run_meta``."""
    conditions, values, meta, meta_values = [], [], [], []
    for key, value in filters.items():
        if key == 'since':
            conditions.append('stamp >= ?')
            values.append(value)
        elif key == 'before':
            conditions.append('stamp < ?')
            values.append(value)
        elif key == 'status':
            conditions.append("json_extract(payload,'$.status') IN (" + ','.join('?' * len(value)) + ')')
            values.extend(value)
        elif key == 'workflow_id':
            meta.append('workflow_id=?')
            meta_values.append(value)
        elif key == 'repo':
            meta.append('repo_path=?')
            meta_values.append(value)
        elif key == 'title_contains':
            meta.append('instr(name_folded,?)>0')
            meta_values.append(value)
        elif key == 'scope':
            meta.append('is_root=?')
            meta_values.append(int(value == 'root'))
    if meta:
        conditions.append('EXISTS (SELECT 1 FROM run_meta WHERE run_meta.id=entries.id AND ' + ' AND '.join(meta) + ')')
        values.extend(meta_values)
    return conditions, values


class Catalog:
    def __init__(self, directory: Path, suffix: str, *, layout: StateLayout | None = None):
        self.state_layout = layout if layout is not None else current_owned().layout
        self.directory, self.suffix = directory, suffix
        self._budget = _request_budget.get() or MetadataBudget()
        self.deferred_headers: set[str] = set()
        with state_session(self.state_layout):
            require_state_path(self.state_layout, directory)
            self.identity = hashlib.sha256(str(directory.resolve()).encode()).hexdigest()[:16]

    @property
    def metadata_bytes(self):
        return self._budget.metadata_bytes

    @metadata_bytes.setter
    def metadata_bytes(self, value):
        self._budget.metadata_bytes = value

    @property
    def metadata_decodes(self):
        return self._budget.metadata_decodes

    @metadata_decodes.setter
    def metadata_decodes(self, value):
        self._budget.metadata_decodes = value

    @property
    def metadata_limit(self):
        return self._budget.metadata_limit

    @metadata_limit.setter
    def metadata_limit(self, value):
        self._budget.metadata_limit = value

    def share_budget(self, other: Catalog):
        self._budget = other._budget

    @layout_operation
    def state(self, db: sqlite3.Connection | None = None) -> dict[str, Any]:
        if db is None:
            with self.connect() as connection:
                self._discover(connection)
                return self.state(connection)
        pending = db.execute('SELECT COUNT(*) FROM pending').fetchone()[0]
        blocked = db.execute("SELECT COUNT(*) FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1").fetchone()[0]
        values = dict(db.execute('SELECT key,value FROM state'))
        complete = values.get('complete') == '1' and values.get('directory_mtime') == str(self.directory.stat().st_mtime_ns)
        status = 'blocked' if blocked else 'preparing' if pending or not complete else 'ready'
        result = {'status': status, 'source': 'task_catalog' if self.suffix == '.meta.json' else 'workflow_catalog',
                  'pending_records': pending, 'blocked_records': blocked}
        if blocked:
            result['blocked_sources'] = [dict(id=row[0], reason=row[1], code=row[2]) for row in db.execute("SELECT id,json_extract(payload,'$.note'),json_extract(payload,'$.task_error.code') FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1 ORDER BY id LIMIT 20")]
            result['blocker_types'] = [row[0] for row in db.execute("SELECT DISTINCT COALESCE(json_extract(payload,'$.index_blocker'),'direct_inspection') FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1 LIMIT 10")]
        if status != 'ready':
            result['reason'] = 'Metadata or caller identity requires direct inspection' if blocked else 'Preparing bounded metadata index'
        return result

    @layout_operation
    def ready(self) -> bool:
        return self.state()['status'] == 'ready'

    @layout_operation
    def invalidate(self, identifier: str) -> None:
        """Repair derivative failures and commit invalidation before replacing metadata."""
        with catalog_lock(self.directory, layout=self.state_layout):
            try:
                self._invalidate_locked(identifier)
            except sqlite3.Error:
                logging.getLogger(__name__).warning('Rebuilding catalog after invalidation transaction failure', exc_info=True)
                try:
                    self._discard_derivative_locked()
                    self._invalidate_locked(identifier)
                except (OSError, sqlite3.Error):
                    logging.getLogger(__name__).error('Catalog invalidation recovery failed; metadata replacement refused', exc_info=True)
                    raise

    def _invalidate_locked(self, identifier: str) -> None:
        assert_owned(self.state_layout)
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO pending VALUES (?)', (identifier,))
            db.execute("DELETE FROM state WHERE key='complete'")

    def _source_stat(self, path):
        return path.lstat() if self.suffix == '.json' else path.stat()

    def _invoke_loader(self, identifier, loader, **kwargs):
        from .workflow_run_documents import WorkflowRunDocumentError
        try:
            return loader(identifier, **kwargs)
        except WorkflowRunDocumentError as exc:
            # A refused source carries no ownership, lineage, session or outcome.
            return ({'workflow_run_id': identifier, 'needs_direct_lookup': True,
                     'index_blocker': exc.code, 'diagnostic': exc.as_dict()}, 0.0, True)

    def _load_bounded(self, identifier: str, loader):
        assert_owned(self.state_layout)
        path = self.directory / (identifier + self.suffix)
        try:
            initial_source = self._source_stat(path)
            size = initial_source.st_size
            if self.suffix == '.json' and not stat.S_ISREG(initial_source.st_mode):
                return self._invoke_loader(identifier, loader, _metadata_budget=self)
        except FileNotFoundError:
            return None
        except OSError:
            if self.suffix in {".meta.json", ".json"}:
                return self._invoke_loader(identifier, loader, _metadata_budget=self)
            raise
        def placeholder():
            key = 'task_id' if self.suffix == '.meta.json' else 'workflow_run_id'
            return {key: identifier, 'status': 'unknown', 'needs_direct_lookup': True,
                    'index_blocker': 'oversized_metadata', 'note': 'metadata exceeds listing read budget; inspect directly'}, 0.0, True
        if size > METADATA_BYTES:
            return placeholder()
        if self.metadata_decodes >= BOOTSTRAP_LIMIT or self.metadata_bytes + size > self.metadata_limit:
            raise DeferredRead()
        if getattr(loader, 'bounded_metadata', False):
            from .bounded_io import ReadLimit
            try:
                value = self._invoke_loader(identifier, loader, _metadata_budget=self)
            except ReadLimit:
                # The opened file may have grown since stat. Only a per-record breach
                # is persistent; aggregate exhaustion must be retried automatically.
                try:
                    if self._source_stat(path).st_size > METADATA_BYTES:
                        return placeholder()
                except FileNotFoundError:
                    raise DeferredRead() from None
                raise DeferredRead() from None
        else:
            self.metadata_decodes += 1
            self.metadata_bytes += size
            value = self._invoke_loader(identifier, loader)
        try:
            final_source = self._source_stat(path)
        except FileNotFoundError:
            raise DeferredRead() from None
        except OSError:
            if self.suffix in {".meta.json", ".json"}:
                return self._invoke_loader(identifier, loader, _metadata_budget=self)
            raise
        if source_identity(initial_source) != source_identity(final_source):
            raise DeferredRead()
        if value is not None:
            value[0]['_source_identity'] = source_identity(initial_source)
        return value

    @layout_operation
    def connect(self) -> sqlite3.Connection:
        lock = catalog_lock(self.directory, layout=self.state_layout)
        lock.__enter__()
        db = None
        try:
            try:
                db = self._connect_locked()
            except sqlite3.Error:
                # The failed handle is closed by _connect_locked. All supported
                # readers/writers share this lock, including sidecar recovery.
                logging.getLogger(__name__).warning('Rebuilding unusable derivative catalog', exc_info=True)
                try:
                    self._discard_derivative_locked()
                    db = self._connect_locked()
                except (OSError, sqlite3.Error):
                    logging.getLogger(__name__).error('Derivative catalog recovery failed; authority unavailable', exc_info=True)
                    raise
            db._catalog_lock = lock
            return db
        except BaseException:
            if db is not None:
                db.close()
            lock.__exit__(None, None, None)
            raise

    def _discard_derivative_locked(self) -> None:
        """Drop only this derivative namespace, never metadata or older catalogs."""
        assert_owned(self.state_layout)
        database = catalog_path(self.directory)
        for suffix in ('', '-journal', '-wal', '-shm'):
            Path(str(database) + suffix).unlink(missing_ok=True)
        # Make the removal durable before any authoritative replacement starts.
        descriptor = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _connect_locked(self) -> sqlite3.Connection:
        assert_owned(self.state_layout)
        db = bind_connection(sqlite3.connect(catalog_path(self.directory), timeout=10, factory=CatalogConnection), self.state_layout)
        try:
            return self._initialize(db)
        except BaseException:
            db.close()
            raise

    def _initialize(self, db: sqlite3.Connection) -> sqlite3.Connection:
        assert_owned(self.state_layout)
        db.execute('PRAGMA journal_mode=PERSIST')
        db.execute('CREATE TABLE IF NOT EXISTS entries (id TEXT PRIMARY KEY, stamp REAL NOT NULL, active INTEGER NOT NULL, session TEXT, payload TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS task_groups (id TEXT PRIMARY KEY, label TEXT NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS task_group_label ON task_groups(label)')
        db.execute('CREATE INDEX IF NOT EXISTS chronology ON entries(stamp DESC,id DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS active_chronology ON entries(active,stamp DESC,id DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS session_chronology ON entries(session,stamp DESC,id DESC)')
        db.execute('CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS callers (id TEXT PRIMARY KEY,pid INTEGER,pgid INTEGER,terminal INTEGER NOT NULL,payload TEXT NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS caller_pid ON callers(pid)')
        db.execute('CREATE INDEX IF NOT EXISTS caller_pgid ON callers(pgid)')
        db.execute('CREATE TABLE IF NOT EXISTS associations (id TEXT PRIMARY KEY,payload TEXT NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS sources (id TEXT PRIMARY KEY,mtime INTEGER,size INTEGER,inode INTEGER,device INTEGER)')
        db.execute('CREATE TABLE IF NOT EXISTS pending (id TEXT PRIMARY KEY)')
        db.execute('CREATE TABLE IF NOT EXISTS seen (id TEXT PRIMARY KEY,generation INTEGER NOT NULL)')
        db.execute('CREATE TABLE IF NOT EXISTS checkout_tasks (id TEXT PRIMARY KEY,run_id TEXT NOT NULL,repo TEXT NOT NULL,payload TEXT NOT NULL)')
        # Run-tree links, indexed so tree discovery never loads or decodes a run record.
        db.execute('CREATE TABLE IF NOT EXISTS workflow_links (id TEXT PRIMARY KEY,root_workflow_run_id TEXT,parent_workflow_run_id TEXT)')
        db.execute('CREATE INDEX IF NOT EXISTS workflow_link_root ON workflow_links(root_workflow_run_id)')
        db.execute('CREATE INDEX IF NOT EXISTS workflow_link_parent ON workflow_links(parent_workflow_run_id)')
        # Lossless run filter columns, independent of header truncation.
        db.execute('CREATE TABLE IF NOT EXISTS run_meta (id TEXT PRIMARY KEY,workflow_id TEXT,repo_path TEXT NOT NULL,name_folded TEXT NOT NULL,is_root INTEGER NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS run_meta_workflow ON run_meta(workflow_id)')
        db.execute('CREATE INDEX IF NOT EXISTS run_meta_repo ON run_meta(repo_path)')
        # Persisted task usage-limit evidence, newest first. Reads take a bounded window of the newest
        # rows; backend and model bound that window through their own indexes, repo and group filter inside it.
        db.execute('CREATE TABLE IF NOT EXISTS task_limit_hits (id TEXT PRIMARY KEY,observed_at REAL NOT NULL,backend TEXT NOT NULL,model TEXT,grp TEXT,repo_path TEXT NOT NULL,payload TEXT NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS task_limit_hit_chronology ON task_limit_hits(observed_at DESC,id DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS task_limit_hit_backend ON task_limit_hits(backend,observed_at DESC,id DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS task_limit_hit_backend_model ON task_limit_hits(backend,model,observed_at DESC,id DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS task_limit_hit_model ON task_limit_hits(model,observed_at DESC,id DESC)')
        # Workflow usage-limit evidence per attempt, and the runs blocked on a usage-limit recovery. The
        # workflow, repo and root columns repeat the run's lossless metadata so each filter has an ordered
        # index; a query bounds its window with one of them.
        db.execute('CREATE TABLE IF NOT EXISTS run_limit_hits (run_id TEXT NOT NULL,hit_key TEXT NOT NULL,root_id TEXT NOT NULL,workflow_id TEXT,repo_path TEXT NOT NULL,backend TEXT NOT NULL,requested_model TEXT,observed_model TEXT,finished_at REAL NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(run_id,hit_key))')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_hit_chronology ON run_limit_hits(finished_at DESC,run_id DESC,hit_key DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_hit_workflow ON run_limit_hits(workflow_id,finished_at DESC,run_id DESC,hit_key DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_hit_repo ON run_limit_hits(repo_path,finished_at DESC,run_id DESC,hit_key DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_hit_root ON run_limit_hits(root_id,finished_at DESC,run_id DESC,hit_key DESC)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_hit_run ON run_limit_hits(run_id,finished_at DESC,hit_key DESC)')
        # One row per run that kept fewer hits than it recorded; keys_exceeded says a key was dropped too.
        db.execute('CREATE TABLE IF NOT EXISTS run_limit_meta (run_id TEXT PRIMARY KEY,root_id TEXT NOT NULL,workflow_id TEXT,repo_path TEXT NOT NULL,keys_exceeded INTEGER NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_meta_keys ON run_limit_meta(keys_exceeded)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_meta_workflow ON run_limit_meta(workflow_id,keys_exceeded)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_meta_repo ON run_limit_meta(repo_path,keys_exceeded)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_meta_root ON run_limit_meta(root_id,keys_exceeded)')
        db.execute('CREATE TABLE IF NOT EXISTS run_limit_blocking (run_id TEXT PRIMARY KEY,root_id TEXT NOT NULL,workflow_id TEXT,repo_path TEXT NOT NULL,payload TEXT NOT NULL)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_blocking_workflow ON run_limit_blocking(workflow_id,run_id)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_blocking_repo ON run_limit_blocking(repo_path,run_id)')
        db.execute('CREATE INDEX IF NOT EXISTS run_limit_blocking_root ON run_limit_blocking(root_id,run_id)')
        db.execute('CREATE INDEX IF NOT EXISTS checkout_repo ON checkout_tasks(repo)')
        db.execute('CREATE INDEX IF NOT EXISTS checkout_run ON checkout_tasks(run_id)')
        version = db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone()
        # Version 9 adds lossless bounded group matching independently of display
        # truncation; version 10 adds the indexed run-tree links; version 11 adds the
        # lossless run filter columns. Older derivative projections rebuild through
        # bounded discovery.
        if version != ('11',):
            db.execute('DELETE FROM workflow_links')
            db.execute('DELETE FROM run_meta')
            db.execute('DELETE FROM task_limit_hits')
            db.execute('DELETE FROM run_limit_hits')
            db.execute('DELETE FROM run_limit_blocking')
            db.execute('DELETE FROM run_limit_meta')
            db.execute('DELETE FROM task_groups')
            db.execute('DELETE FROM checkout_tasks')
            db.execute('DELETE FROM entries')
            db.execute('DELETE FROM callers')
            db.execute('DELETE FROM associations')
            db.execute('DROP TABLE sources')
            db.execute('CREATE TABLE sources (id TEXT PRIMARY KEY,mtime INTEGER,size INTEGER,inode INTEGER,device INTEGER)')
            db.execute('DELETE FROM pending')
            db.execute('DELETE FROM seen')
            db.execute('DELETE FROM state')
            db.execute("INSERT INTO state VALUES ('schema_version','11')")
        if self.suffix == '.meta.json' and db.execute("SELECT value FROM state WHERE key='task_projection_version'").fetchone() != (TASK_PROJECTION_VERSION,):
            for table in ('entries', 'callers', 'task_groups', 'checkout_tasks', 'associations', 'sources', 'seen', 'pending', 'task_limit_hits', 'run_limit_hits', 'run_limit_blocking', 'run_limit_meta'):
                db.execute(f'DELETE FROM {table}')
            db.execute("DELETE FROM state WHERE key!='schema_version'")
            db.execute("INSERT INTO state VALUES ('task_projection_version',?)", (TASK_PROJECTION_VERSION,))
        if self.suffix == '.json' and db.execute("SELECT value FROM state WHERE key='run_projection_version'").fetchone() != (RUN_PROJECTION_VERSION,):
            for table in ('entries', 'callers', 'task_groups', 'checkout_tasks', 'associations', 'sources', 'seen', 'pending', 'workflow_links', 'run_meta', 'task_limit_hits', 'run_limit_hits', 'run_limit_blocking', 'run_limit_meta'):
                db.execute(f'DELETE FROM {table}')
            db.execute("DELETE FROM state WHERE key!='schema_version'")
            db.execute("INSERT INTO state VALUES ('run_projection_version',?)", (RUN_PROJECTION_VERSION,))
        # Build only after invalidating old derivative headers. The partial index
        # makes warm authority readiness independent of retained history size.
        db.execute("CREATE INDEX IF NOT EXISTS incomplete_headers ON entries(id) WHERE json_extract(payload,'$.needs_direct_lookup')=1")
        db.execute("CREATE INDEX IF NOT EXISTS active_historical ON entries(id) WHERE active=1 AND COALESCE(json_extract(payload,'$.kind'),'workflow')!='builder' AND COALESCE(json_extract(payload,'$.execution_contract'),'')!='delegation'")
        db.commit()
        return db

    def _put(self, db: sqlite3.Connection, header: dict[str, Any], stamp: float, active: bool, identifier: str) -> None:
        # A legacy writer can remove or replace metadata despite our advisory
        # lock. Publish all derivative authority only after final validation.
        assert_owned(self.state_layout)
        db.execute('SAVEPOINT metadata_projection')
        try:
            self._put_projection(db, header, stamp, active, identifier)
        except BaseException:
            db.execute('ROLLBACK TO metadata_projection')
            db.execute('RELEASE metadata_projection')
            raise
        db.execute('RELEASE metadata_projection')

    def _put_projection(self, db: sqlite3.Connection, header: dict[str, Any], stamp: float, active: bool, identifier: str) -> None:
        assert_owned(self.state_layout)
        header = dict(header)
        expected_source = header.pop('_source_identity', None)
        if expected_source is not None:
            try:
                if tuple(expected_source) != source_identity(self._source_stat(self.directory / (identifier + self.suffix))):
                    raise DeferredRead()
            except OSError:
                if not (header.get("index_blocker") in {"unavailable_metadata", "workflow_run_unavailable"}):
                    raise DeferredRead() from None
                expected_source = None
        checkout = header.pop('_checkout', [])
        db.execute('DELETE FROM checkout_tasks WHERE run_id=?', (identifier,))
        for task in checkout:
            encoded = json.dumps(task, ensure_ascii=True, separators=(',', ':'))
            if len(encoded.encode()) > 8 * 1024:
                header.update(needs_direct_lookup=True, status='unknown')
                continue
            db.execute('INSERT OR REPLACE INTO checkout_tasks VALUES (?,?,?,?)', (task['task_id'], identifier, task['repo_path'], encoded))
        if self.suffix == '.meta.json' and header.get('needs_direct_lookup'):
            db.execute('DELETE FROM callers WHERE id=?', (identifier,))
            db.execute('DELETE FROM associations WHERE id=?', (identifier,))
        caller = header.pop('_caller', None)
        associations = header.pop('_associations', {})
        db.execute("DELETE FROM associations WHERE json_extract(payload,'$.workflow_run_id')=?", (identifier,))
        for task_id, association in associations.items():
            db.execute('INSERT OR REPLACE INTO associations VALUES (?,?)', (task_id, json.dumps(association, separators=(',', ':'))))
        if caller is not None:
            # Identity must be lossless; display/content fields never enter the caller index.
            keys = {'task_id', 'backend', 'session_id', 'started_at', 'freedom', 'markers', 'pid', 'pgid', 'start_time', 'status', 'exit_code', 'root_task_id', 'parent_task_id', 'continuation_of_task_id', 'chain_root_task_id', 'spawned_by', 'depth', 'max_depth', 'network'}
            caller = {key: value for key, value in caller.items() if key in keys}
            caller['repo_path'] = ''
            markers = caller.get('markers')
            supported = isinstance(markers, list) and len(markers) <= 64 and all(isinstance(marker, str) and len(marker) <= 4096 for marker in markers) and sum(len(marker) for marker in markers) <= 4096
            supported = supported and all(value is None or isinstance(value, (bool, int, float)) or isinstance(value, str) and len(value) <= 512 for key, value in caller.items() if key != 'markers')
            encoded = json.dumps(caller, ensure_ascii=True, separators=(',', ':')) if supported else ''
            supported = supported and len(encoded.encode()) <= 8 * 1024 and all(not isinstance(value, float) or math.isfinite(value) for value in caller.values())
            if supported:
                terminal = caller.get('status') in {'completed', 'failed', 'timed_out', 'cancelled'} and caller.get('exit_code') is not None
                db.execute('INSERT INTO callers VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET pid=excluded.pid,pgid=excluded.pgid,terminal=excluded.terminal,payload=excluded.payload',
                           (identifier, caller.get('pid'), caller.get('pgid'), int(terminal), encoded))
            else:
                db.execute('DELETE FROM callers WHERE id=?', (identifier,))
                header.update(status='unknown', needs_direct_lookup=True, index_blocker='unsupported_caller_identity', note='caller identity exceeds bounded index; direct inspection required')
                active = True
        run_meta = header.pop('_run_meta', None)
        if self.suffix == '.json' and run_meta is not None and not header.get('needs_direct_lookup'):
            db.execute('INSERT OR REPLACE INTO run_meta VALUES (?,?,?,?,?)', (identifier, run_meta['workflow_id'], run_meta['repo_path'], run_meta['name_folded'], int(run_meta['is_root'])))
        elif self.suffix == '.json':
            db.execute('DELETE FROM run_meta WHERE id=?', (identifier,))
        if self.suffix == '.json' and not header.get('needs_direct_lookup'):
            links = (header.get('root_workflow_run_id') or None, header.get('parent_workflow_run_id') or None)
            db.execute('INSERT OR REPLACE INTO workflow_links VALUES (?,?,?)', (identifier, *links))
        elif self.suffix == '.json':
            db.execute('DELETE FROM workflow_links WHERE id=?', (identifier,))
        limit_hit = header.pop('_limit_hit', None)
        if self.suffix == '.meta.json':
            # Every put replaces this source's hit, so a record that no longer carries the evidence drops it.
            db.execute('DELETE FROM task_limit_hits WHERE id=?', (identifier,))
        if self.suffix == '.meta.json' and limit_hit is not None and not header.get('needs_direct_lookup'):
            db.execute('INSERT INTO task_limit_hits VALUES (?,?,?,?,?,?,?)', (identifier, limit_hit['observed_at'], limit_hit['backend'], limit_hit['model'], limit_hit['grp'], limit_hit['repo_path'],
                                                                              json.dumps(limit_hit['payload'], ensure_ascii=True, separators=(',', ':'))))
        run_limit_hits = header.pop('_limit_hits', None)
        run_limit_blocking = header.pop('_limit_blocking', None)
        run_limit_meta = header.pop('_limit_meta', None)
        if self.suffix == '.json':
            # Every put replaces this run's rows, so a run that loses its evidence or stops blocking drops them.
            db.execute('DELETE FROM run_limit_hits WHERE run_id=?', (identifier,))
            db.execute('DELETE FROM run_limit_blocking WHERE run_id=?', (identifier,))
            db.execute('DELETE FROM run_limit_meta WHERE run_id=?', (identifier,))
        group = header.get('group')
        db.execute('DELETE FROM task_groups WHERE id=?', (identifier,))
        if isinstance(group, str) and len(group) <= 128:
            db.execute('INSERT INTO task_groups VALUES (?,?)', (identifier, group))
        session_id = header.get('session_id')
        header = bound_header(header)
        if self.suffix == '.json' and header.get('needs_direct_lookup'):
            # A budget placeholder is unknown, so it must not keep filter metadata.
            db.execute('DELETE FROM run_meta WHERE id=?', (identifier,))
        elif self.suffix == '.meta.json' and header.get('needs_direct_lookup'):
            db.execute('DELETE FROM task_limit_hits WHERE id=?', (identifier,))
        if self.suffix == '.json' and not header.get('needs_direct_lookup'):
            for hit in run_limit_hits or ():
                db.execute('INSERT INTO run_limit_hits VALUES (?,?,?,?,?,?,?,?,?,?)', (identifier, hit['hit_key'], hit['root_id'], hit['workflow_id'], hit['repo_path'], hit['backend'], hit['requested_model'],
                                                                                      hit['observed_model'], hit['finished_at'], json.dumps(hit['payload'], ensure_ascii=True, separators=(',', ':'))))
            if run_limit_meta is not None:
                db.execute('INSERT INTO run_limit_meta VALUES (?,?,?,?,?)', (identifier, run_limit_meta['root_id'], run_limit_meta['workflow_id'], run_limit_meta['repo_path'], int(run_limit_meta['keys_exceeded'])))
            if run_limit_blocking is not None:
                db.execute('INSERT INTO run_limit_blocking VALUES (?,?,?,?,?)', (identifier, run_limit_blocking['root_id'], run_limit_blocking['workflow_id'], run_limit_blocking['repo_path'],
                                                                                json.dumps(run_limit_blocking['payload'], ensure_ascii=True, separators=(',', ':'))))
        db.execute("INSERT INTO entries VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET stamp=CASE WHEN json_extract(entries.payload,'$.needs_direct_lookup')=1 THEN excluded.stamp ELSE entries.stamp END,active=excluded.active,session=excluded.session,payload=excluded.payload",
                   (identifier, stamp, int(active), session_id, json.dumps(header, ensure_ascii=True, separators=(',', ':'))))
        generation = db.execute("SELECT value FROM state WHERE key='scan_generation'").fetchone()
        db.execute('INSERT OR REPLACE INTO seen VALUES (?,?)', (identifier, int(generation[0]) if generation else 0))
        try:
            source = self._source_stat(self.directory / f'{identifier}{self.suffix}')
            if expected_source is not None and tuple(expected_source) != source_identity(source):
                raise DeferredRead()
            db.execute('INSERT OR REPLACE INTO sources VALUES (?,?,?,?,?)', (identifier, *(tuple(expected_source) if expected_source is not None else source_identity(source))))
            db.execute('DELETE FROM pending WHERE id=?', (identifier,))
        except OSError:
            if header.get("index_blocker") in {"unavailable_metadata", "workflow_run_unavailable"}:
                db.execute("DELETE FROM sources WHERE id=?", (identifier,))
                db.execute("DELETE FROM pending WHERE id=?", (identifier,))
            else:
                raise DeferredRead() from None

    def _changed(self, db: sqlite3.Connection, identifier: str) -> bool:
        assert_owned(self.state_layout)
        row = db.execute('SELECT mtime,size,inode,device FROM sources WHERE id=?', (identifier,)).fetchone()
        try:
            source = self._source_stat(self.directory / f'{identifier}{self.suffix}')
            placeholder = db.execute("SELECT json_extract(payload,'$.needs_direct_lookup') FROM entries WHERE id=?", (identifier,)).fetchone()
            return row != (source.st_mtime_ns, source.st_size, getattr(source, 'st_ino', 0), getattr(source, 'st_dev', 0))
        except OSError:
            return True

    def _remove(self, db: sqlite3.Connection, identifier: str) -> None:
        assert_owned(self.state_layout)
        db.execute('DELETE FROM checkout_tasks WHERE run_id=?', (identifier,))
        db.execute("DELETE FROM associations WHERE json_extract(payload,'$.workflow_run_id')=?", (identifier,))
        for table in ('entries', 'callers', 'associations', 'sources', 'seen', 'pending', 'task_groups', 'workflow_links', 'run_meta', 'task_limit_hits'):
            db.execute(f'DELETE FROM {table} WHERE id=?', (identifier,))
        for table in ('run_limit_hits', 'run_limit_blocking', 'run_limit_meta'):
            db.execute(f'DELETE FROM {table} WHERE run_id=?', (identifier,))

    def _acknowledge_directory(self, db: sqlite3.Connection, previous_directory_mtime: int | None) -> None:
        # Older processes do not honor our lock. A matching-file replacement may
        # have overlapped this projection, so only discovery can acknowledge a
        # changed namespace. Keep the prior verified stamp and queued work.
        assert_owned(self.state_layout)
        if dict(db.execute('SELECT key,value FROM state')).get('directory_mtime') != str(self.directory.stat().st_mtime_ns):
            db.execute("DELETE FROM state WHERE key='complete'")

    @layout_operation
    def record(self, header: dict[str, Any], stamp: float, active: bool, identifier: str, *, previous_directory_mtime: int | None = None) -> None:
        try:
            with self.connect() as db:
                self._put(db, header, stamp, active, identifier)
                self._acknowledge_directory(db, previous_directory_mtime)
        except (OSError, sqlite3.Error, DeferredRead):
            logging.getLogger(__name__).warning('Listing catalog update unavailable', exc_info=True)

    @layout_operation
    def remove(self, identifier: str, *, previous_directory_mtime: int | None = None) -> None:
        with self.connect() as db:
            self._remove(db, identifier)
            self._acknowledge_directory(db, previous_directory_mtime)

    @layout_operation
    def headers(self, identifiers: list[str], loader: Callable[[str], tuple[dict[str, Any], float, bool] | None]) -> list[dict[str, Any]]:
        if len(identifiers) > PAGE_LIMIT:
            raise ValueError('At most 100 identifiers may be requested')
        result = []
        self.deferred_headers = set()
        self.blocked_headers = {}
        with self.connect() as db:
            self._discover(db)
            for identifier in dict.fromkeys(identifiers):
                row = db.execute('SELECT payload FROM entries WHERE id=?', (identifier,)).fetchone()
                if row is not None and not self._changed(db, identifier):
                    header = json.loads(row[0])
                    if header.get('needs_direct_lookup'):
                        self.blocked_headers[identifier] = header.get('index_blocker', 'direct_inspection')
                    else:
                        result.append(header)
                else:
                    try:
                        value = self._load_bounded(identifier, loader)
                        if value is not None:
                            header, stamp, active = value
                            self._put(db, header, stamp, active, identifier)
                            projected = json.loads(db.execute('SELECT payload FROM entries WHERE id=?', (identifier,)).fetchone()[0])
                            if projected.get('needs_direct_lookup'):
                                self.blocked_headers[identifier] = projected.get('index_blocker', 'direct_inspection')
                            else:
                                result.append(projected)
                        else:
                            self._remove(db, identifier)
                    except DeferredRead:
                        db.execute('INSERT OR IGNORE INTO pending VALUES (?)', (identifier,))
                        self.deferred_headers.add(identifier)
                        # Budget exhaustion is not evidence of an unknown task outcome.
                        # Omit this source until a later read refreshes its projection.

        return result

    @layout_operation
    def requested_preparation(self) -> dict[str, Any]:
        blocked = getattr(self, 'blocked_headers', {})
        state = {'status': 'blocked' if blocked else 'preparing',
                 'source': 'task_catalog' if self.suffix == '.meta.json' else 'workflow_catalog',
                 'reason': 'Metadata or caller identity requires direct inspection' if blocked else 'Preparing requested metadata headers',
                 'pending_records': len(self.deferred_headers), 'blocked_records': len(blocked)}
        if blocked:
            state['blocker_types'] = sorted(set(blocked.values()))
            state['blocked_sources'] = [{'id': identifier, 'reason': reason} for identifier, reason in list(blocked.items())[:20]]
        return state

    @layout_operation
    def header_page(self, identifiers: list[str], loader) -> dict[str, Any]:
        headers = self.headers(identifiers, loader)
        pending = bool(self.deferred_headers or self.blocked_headers)
        page = {'items': [] if pending else headers, 'next_cursor': None, 'has_more': False, 'bootstrap_pending': pending}
        if pending:
            apply_read_state(page, self.requested_preparation())
        return page

    def _discover(self, db: sqlite3.Connection) -> None:
        """Stream changed discovery into SQLite; unrelated churn never resets pending work."""
        assert_owned(self.state_layout)
        directory_mtime = str(self.directory.stat().st_mtime_ns)
        state = dict(db.execute('SELECT key,value FROM state'))
        if state.get('complete') == '1' and state.get('directory_mtime') == directory_mtime:
            return
        generation = int((db.execute("SELECT value FROM state WHERE key='scan_generation'").fetchone() or ('0',))[0]) + 1
        db.execute("INSERT OR REPLACE INTO state VALUES ('scan_generation',?)", (str(generation),))
        with os.scandir(self.directory) as paths:
            for path in paths:
                if not path.name.endswith(self.suffix):
                    continue
                identifier = path.name[:-len(self.suffix)]
                db.execute('INSERT OR REPLACE INTO seen VALUES (?,?)', (identifier, generation))
                if self._changed(db, identifier):
                    db.execute('INSERT OR IGNORE INTO pending VALUES (?)', (identifier,))
        # Uncooperative writers may not take our advisory lock. Never prune or
        # claim readiness from a discovery snapshot changed while enumerating.
        if str(self.directory.stat().st_mtime_ns) != directory_mtime:
            db.execute("DELETE FROM state WHERE key='complete'")
            return
        db.execute("INSERT OR REPLACE INTO state VALUES ('directory_mtime',?)", (directory_mtime,))
        # Only a completed discovery pass authorizes pruning missing metadata.
        # Older interrupted projections may have authority but no source row.
        # Reconcile every projection namespace against the verified discovery.
        stale = db.execute("""WITH projected AS (
            SELECT id FROM sources UNION SELECT id FROM entries UNION SELECT id FROM callers UNION SELECT id FROM task_groups
            UNION SELECT run_id AS id FROM checkout_tasks UNION SELECT id FROM run_meta UNION SELECT id FROM task_limit_hits
            UNION SELECT run_id AS id FROM run_limit_hits UNION SELECT run_id AS id FROM run_limit_blocking UNION SELECT run_id AS id FROM run_limit_meta
            UNION SELECT json_extract(payload,'$.workflow_run_id') AS id FROM associations
        ) SELECT id FROM projected WHERE id IS NOT NULL
          AND id NOT IN (SELECT id FROM seen WHERE generation=?)""", (generation,))
        while rows := stale.fetchmany(100):
            for (identifier,) in rows:
                self._remove(db, identifier)
        db.execute('DELETE FROM pending WHERE id NOT IN (SELECT id FROM seen WHERE generation=?)', (generation,))
        db.execute("INSERT OR REPLACE INTO state VALUES ('complete','1')")

    def limit_hit_window(self, db: sqlite3.Connection, limit: int, filters: dict[str, Any]) -> tuple[list[tuple[float, str, str | None, dict[str, Any]]], int, bool]:
        """Newest usage-limit hits as ``(observed_at, backend, model, payload)``, the cap applied first.

        The window is the newest ``limit`` rows in ``(observed_at, id)`` order that satisfy the
        indexed predicates ``since``, ``backend`` and ``model``; each combination is served by an
        index whose trailing columns are the ordering, so the scan stops after ``limit`` rows. It is
        isolated in a subquery so no other filter can replace its index bounds and many equal
        timestamps cannot widen it. ``repo`` and ``group`` then filter inside that window. Returns
        the rows, the rows examined and whether older matching rows were left unexamined.
        """
        bounds, bound_values = [], []
        for key, column, operator in (('backend', 'backend', '='), ('model', 'model', '='), ('since', 'observed_at', '>=')):
            if key in filters:
                bounds.append(f'{column} {operator} ?')
                bound_values.append(filters[key])
        bound = 'WHERE ' + ' AND '.join(bounds) + ' ' if bounds else ''
        total = db.execute('SELECT COUNT(*) FROM (SELECT 1 FROM task_limit_hits ' + bound + 'ORDER BY observed_at DESC,id DESC LIMIT ?)', (*bound_values, limit + 1)).fetchone()[0]
        conditions, values = [], []
        for key, column in (('repo', 'repo_path'), ('group', 'grp')):
            if key in filters:
                conditions.append(f't.{column}=?')
                values.append(filters[key])
        rows = db.execute('SELECT t.observed_at,t.backend,t.model,t.payload FROM (SELECT id AS wid FROM task_limit_hits ' + bound + 'ORDER BY observed_at DESC,id DESC LIMIT ?) JOIN task_limit_hits t ON t.id=wid'
                          + (' WHERE ' + ' AND '.join(conditions) if conditions else '') + ' ORDER BY t.observed_at DESC,t.id DESC', (*bound_values, limit, *values)).fetchall()
        return [(stamp, backend, model, json.loads(payload)) for stamp, backend, model, payload in rows], min(total, limit), total > limit

    @staticmethod
    def _run_limit_bound(filters: dict[str, Any], indexes: dict[str, tuple[str, str]], default: str) -> tuple[str, str, list[Any], list[tuple[str, Any]]]:
        """The index that bounds a run limit query, its equality predicate and the filters left for the outer query.

        ``root_workflow_run_id``, ``workflow_id`` and ``repo`` each have an ordered index; the first present
        (in that order) bounds the window and the others filter inside it.
        """
        for key in ('root_workflow_run_id', 'workflow_id', 'repo'):
            if key in filters:
                index, column = indexes[key]
                rest = [(indexes[other][1], filters[other]) for other in ('root_workflow_run_id', 'workflow_id', 'repo') if other in filters and other != key]
                return index, f'{column}=?', [filters[key]], rest
        return default, '', [], []

    def run_limit_hit_window(self, db: sqlite3.Connection, limit: int, filters: dict[str, Any], run_ids: list[str] | None = None) -> tuple[list[tuple[float, str, str, str | None, str | None, str, dict[str, Any]]], int, bool]:
        """Newest workflow usage-limit hits as ``(finished_at, run_id, backend, requested_model, observed_model, root_id, payload)``.

        A direct query takes the newest ``limit`` rows in ``(finished_at, run_id, hit_key)`` order that
        satisfy one indexed bound (the root, else the workflow, else the repo, plus the ``since`` and
        ``before`` range on ``finished_at``), isolated in a subquery so no other predicate can change the
        scan; the remaining equality filters apply inside that window. A query for ``run_ids`` (a managed
        caller's own run and descendants) starts from at most ``RUN_HIT_LIMIT`` rows per run and applies
        every filter before the cap. Returns the rows, the rows examined and whether more were left.
        """
        indexes = {'root_workflow_run_id': ('run_limit_hit_root', 'root_id'), 'workflow_id': ('run_limit_hit_workflow', 'workflow_id'), 'repo': ('run_limit_hit_repo', 'repo_path')}
        range_conditions, range_values = [], []
        for key, operator in (('since', '>='), ('before', '<')):
            if key in filters:
                range_conditions.append(f'finished_at {operator} ?')
                range_values.append(filters[key])
        columns = 'finished_at,run_id,backend,requested_model,observed_model,root_id,payload'
        order = 'ORDER BY finished_at DESC,run_id DESC,hit_key DESC'
        if run_ids is not None:
            conditions = ['run_id IN (' + ','.join('?' * len(run_ids)) + ')', *range_conditions]
            values: list[Any] = [*run_ids, *range_values]
            for key, column in (('root_workflow_run_id', 'root_id'), ('workflow_id', 'workflow_id'), ('repo', 'repo_path')):
                if key in filters:
                    conditions.append(f'{column}=?')
                    values.append(filters[key])
            where = ' WHERE ' + ' AND '.join(conditions)
            source = 'FROM run_limit_hits INDEXED BY run_limit_hit_run' + where + ' ' + order
            total = db.execute('SELECT COUNT(*) FROM (SELECT 1 ' + source + ' LIMIT ?)', (*values, limit + 1)).fetchone()[0]
            rows = db.execute('SELECT ' + columns + ' ' + source + ' LIMIT ?', (*values, limit)).fetchall()
        else:
            index, equality, equality_values, rest = self._run_limit_bound(filters, indexes, 'run_limit_hit_chronology')
            bound_conditions = ([equality] if equality else []) + range_conditions
            bound = ' WHERE ' + ' AND '.join(bound_conditions) if bound_conditions else ''
            bound_values = [*equality_values, *range_values]
            source = 'FROM run_limit_hits INDEXED BY ' + index + bound + ' ' + order
            total = db.execute('SELECT COUNT(*) FROM (SELECT 1 ' + source + ' LIMIT ?)', (*bound_values, limit + 1)).fetchone()[0]
            outer = ''.join(f' AND t.{column}=?' for column, _ in rest)
            rows = db.execute('SELECT t.finished_at,t.run_id,t.backend,t.requested_model,t.observed_model,t.root_id,t.payload FROM (SELECT run_id AS wr,hit_key AS wk ' + source + ' LIMIT ?) '
                              'CROSS JOIN run_limit_hits t ON t.run_id=wr AND t.hit_key=wk WHERE 1=1' + outer + ' ORDER BY t.finished_at DESC,t.run_id DESC,t.hit_key DESC',
                              (*bound_values, limit, *[value for _, value in rest])).fetchall()
        return [(*row[:6], json.loads(row[6])) for row in rows], min(total, limit), total > limit

    def run_limit_blocking_window(self, db: sqlite3.Connection, limit: int, filters: dict[str, Any], run_ids: list[str] | None = None) -> tuple[list[tuple[str, str, dict[str, Any]]], bool]:
        """Runs blocked on a usage-limit recovery as ``(run_id, root_id, payload)`` in run id order.

        Bounded like ``run_limit_hit_window``: an indexed root, workflow or repo bound selects the first
        ``limit`` rows and the other equality filters apply inside them; ``since`` and ``before`` do not
        apply, since a blocking row has no time of its own. Returns the rows and whether more were left.
        """
        indexes = {'root_workflow_run_id': ('run_limit_blocking_root', 'root_id'), 'workflow_id': ('run_limit_blocking_workflow', 'workflow_id'), 'repo': ('run_limit_blocking_repo', 'repo_path')}
        if run_ids is not None:
            conditions, values = ['run_id IN (' + ','.join('?' * len(run_ids)) + ')'], [*run_ids]
            for key, column in (('root_workflow_run_id', 'root_id'), ('workflow_id', 'workflow_id'), ('repo', 'repo_path')):
                if key in filters:
                    conditions.append(f'{column}=?')
                    values.append(filters[key])
            rows = db.execute('SELECT run_id,root_id,payload FROM run_limit_blocking WHERE ' + ' AND '.join(conditions) + ' ORDER BY run_id LIMIT ?', (*values, limit + 1)).fetchall()
            capped = len(rows) > limit
        else:
            index, equality, equality_values, rest = self._run_limit_bound(filters, indexes, '')
            source = 'FROM run_limit_blocking' + (f' INDEXED BY {index} WHERE {equality}' if equality else '') + ' ORDER BY run_id'
            capped = db.execute('SELECT COUNT(*) FROM (SELECT 1 ' + source + ' LIMIT ?)', (*equality_values, limit + 1)).fetchone()[0] > limit
            outer = ''.join(f' AND t.{column}=?' for column, _ in rest)
            rows = db.execute('SELECT t.run_id,t.root_id,t.payload FROM (SELECT run_id AS wr ' + source + ' LIMIT ?) CROSS JOIN run_limit_blocking t ON t.run_id=wr WHERE 1=1' + outer + ' ORDER BY t.run_id',
                              (*equality_values, limit, *[value for _, value in rest])).fetchall()
        return [(run_id, root_id, json.loads(payload)) for run_id, root_id, payload in rows[:limit]], capped

    def run_limit_truncated(self, db: sqlite3.Connection, filters: dict[str, Any], run_ids: list[str] | None = None, *, any_truncation: bool = False) -> bool:
        """Whether a run in scope dropped hits that could change the answer.

        Only runs over the retention cap have a ``run_limit_meta`` row. A direct query looks at the same
        scope as its hit window (the first of root, workflow and repo, else everything) with an indexed
        ``LIMIT 1``; a managed query looks up its verified run ids by primary key. Unless
        ``any_truncation`` is set (a ``since`` or ``before`` bound can hide dropped older hits), only a
        run that also dropped a whole key counts.
        """
        extra = '' if any_truncation else 'keys_exceeded=1'
        if run_ids is not None:
            where = ['run_id IN (' + ','.join('?' * len(run_ids)) + ')'] + ([extra] if extra else [])
            return db.execute('SELECT 1 FROM run_limit_meta WHERE ' + ' AND '.join(where) + ' LIMIT 1', run_ids).fetchone() is not None
        for key, column, index in (('root_workflow_run_id', 'root_id', 'run_limit_meta_root'), ('workflow_id', 'workflow_id', 'run_limit_meta_workflow'), ('repo', 'repo_path', 'run_limit_meta_repo')):
            if key in filters:
                return db.execute(f'SELECT 1 FROM run_limit_meta INDEXED BY {index} WHERE {column}=?' + (f' AND {extra}' if extra else '') + ' LIMIT 1', (filters[key],)).fetchone() is not None
        if any_truncation:
            return db.execute('SELECT 1 FROM run_limit_meta LIMIT 1').fetchone() is not None
        return db.execute('SELECT 1 FROM run_limit_meta INDEXED BY run_limit_meta_keys WHERE keys_exceeded=1 LIMIT 1').fetchone() is not None

    def incomplete_sources(self, db: sqlite3.Connection, limit: int) -> tuple[list[tuple[str, str | None, str | None]], bool]:
        """Up to ``limit`` ids whose header needs direct inspection, with ``(id, reason, blocker)``, from the partial index."""
        rows = db.execute("SELECT id,json_extract(payload,'$.note'),json_extract(payload,'$.index_blocker') FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1 ORDER BY id LIMIT ?", (limit + 1,)).fetchall()
        return rows[:limit], len(rows) > limit

    @layout_operation
    def prefix_names(self, prefix: str, limit: int) -> dict[str, Any] | None:
        """Names of the completed current discovery generation that start with ``prefix``.

        Discovery must complete; it is retried once when concurrent churn clears the completion
        mark, and ``None`` means it still did not complete, so uniqueness cannot be decided.
        Uniqueness comes only from ``seen`` rows of the current scan generation, matched by a
        primary-key range. Stale ``entries`` are used only for candidate display metadata.
        """
        if not prefix:
            raise ValueError('prefix must be nonempty')
        upper = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        with self.connect() as db:
            for _ in range(2):
                self._discover(db)
                values = dict(db.execute('SELECT key,value FROM state'))
                if values.get('complete') == '1' and values.get('directory_mtime') == str(self.directory.stat().st_mtime_ns):
                    break
            else:
                return None
            generation = int(values.get('scan_generation', '0'))
            names = [row[0] for row in db.execute('SELECT id FROM seen WHERE generation=? AND id>=? AND id<? ORDER BY id LIMIT ?', (generation, prefix, upper, limit))]
            headers: dict[str, dict[str, Any]] = {}
            for identifier in names:
                row = db.execute('SELECT stamp,payload FROM entries WHERE id=?', (identifier,)).fetchone()
                if row is not None:
                    try:
                        header = json.loads(row[1])
                    except ValueError:
                        continue
                    if isinstance(header, dict):
                        headers[identifier] = {**header, '_stamp': row[0]}
            return {'names': names, 'headers': headers}

    @span("catalog")
    def bootstrap(self, db: sqlite3.Connection, loader: Callable[[str], tuple[dict[str, Any], float, bool] | None]) -> bool:
        self._discover(db)
        identifiers = db.execute('SELECT id FROM pending ORDER BY id LIMIT ?', (BOOTSTRAP_LIMIT,)).fetchall()
        for (identifier,) in identifiers:
            try:
                value = self._load_bounded(identifier, loader)
            except DeferredRead:
                break
            except (OSError, ValueError, KeyError, TypeError):
                logging.getLogger(__name__).warning('Unreadable listing record %s', identifier)
                value = ({'task_id' if self.suffix == '.meta.json' else 'workflow_run_id': identifier,
                          'status': 'unknown', 'needs_direct_lookup': True,
                          'index_blocker': 'unreadable_metadata', 'note': 'Metadata is unreadable; inspect directly'}, 0.0, True)
            if value is None and (self.directory / (identifier + self.suffix)).exists():
                key = 'task_id' if self.suffix == '.meta.json' else 'workflow_run_id'
                value = ({key: identifier, 'status': 'unknown', 'needs_direct_lookup': True,
                          'index_blocker': 'unreadable_metadata', 'note': 'Metadata is unreadable; inspect directly'}, 0.0, True)
            if value is not None:
                try:
                    self._put(db, *value, identifier)
                except DeferredRead:
                    continue
            else:
                self._remove(db, identifier)
        state = self.state(db)
        return bool(state['pending_records']) or state['status'] == 'preparing'

    @layout_operation
    def page(self, loader: Callable[[str], tuple[dict[str, Any], float, bool] | None], *, limit: int = PAGE_LIMIT,
             cursor: str | None = None, active_only: bool = False, session_id: str | None = None, filters: dict[str, Any] | None = None,
             validator: Callable[[dict[str, Any]], dict[str, Any]] | None = None) -> dict[str, Any]:
        if validator is None:
            from .task_reads import validate_filters as validator
        filters = validator(filters or {})
        # Run filters (some cannot use an index) examine a bounded window of rows per page.
        windowed = self.suffix == '.json' and bool(filters)
        if type(limit) is not int or not 1 <= limit <= PAGE_LIMIT:
            raise ValueError('limit must be 1–100')
        if not isinstance(active_only, bool) or session_id is not None and (not isinstance(session_id, str) or len(session_id) > 256):
            raise ValueError('active_only must be boolean; session_id must be a bounded string')
        # Keep cursor size independent of UTF-8 filter values and JSON escaping.
        # Canonical ordering binds equivalent filter maps to the same scope.
        filter_scope = hashlib.sha256(json.dumps(filters, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest() if filters else None
        scope = [self.identity, active_only, session_id, filter_scope] if filters else [self.identity, active_only, session_id]
        boundary = None
        if cursor is not None:
            try:
                if not isinstance(cursor, str) or len(cursor) > 2048:
                    raise ValueError()
                value = json.loads(base64.urlsafe_b64decode(cursor.encode()))
                if not isinstance(value, dict) or value.get('version') != 1 or value['scope'] != scope or type(value['stamp']) not in (float, int) or not math.isfinite(value['stamp']) or not isinstance(value['id'], str) or len(value['id']) > 100:
                    raise ValueError()
                boundary = (value['stamp'], value['id'])
            except (ValueError, KeyError, TypeError):
                raise ValueError('Invalid listing cursor') from None
        with self.connect() as db:
            pending = self.bootstrap(db, loader)
            incomplete = db.execute("SELECT 1 FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1 LIMIT 1").fetchone() is not None
            conditions, values = [], []
            scoped, scoped_values = [], []
            if session_id is not None:
                conditions.append('session=?')
                values.append(session_id)
                scoped.append('session=?')
                scoped_values.append(session_id)
            if windowed:
                filter_conditions, filter_values = _run_filter_conditions(filters)
                conditions.extend(filter_conditions)
                values.extend(filter_values)
            for key, value in ([] if windowed else filters.items()):
                if key == 'predecessor_task_id':
                    conditions.append("(json_extract(payload,'$.parent_task_id') = ? OR json_extract(payload,'$.continuation_of_task_id') = ?)")
                    values.extend((value, value))
                elif key == 'since':
                    conditions.append('stamp >= ?')
                    values.append(value)
                elif key == 'group':
                    conditions.append('id IN (SELECT id FROM task_groups WHERE label=?)')
                    values.append(value)
                elif key == 'title_contains':
                    conditions.append("instr(lower(COALESCE(json_extract(payload,'$.title'),'')),lower(?)) > 0")
                    values.append(value)
                else:
                    field = 'repo_path' if key == 'repo' else key
                    conditions.append("json_extract(payload,'$." + field + "') = ?")
                    values.append(value)
            base = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
            # Filtered run pages never compute a filtered total (it would scan the whole catalog).
            active_count = None if windowed else db.execute('SELECT COUNT(*) FROM entries' + base + (' AND active=1' if conditions else ' WHERE active=1'), values).fetchone()[0]
            count_values = list(values)
            root_counts = db.execute("SELECT COUNT(*),COALESCE(SUM(json_extract(payload,'$.status') IN ('needs_attention','needs_input')),0) FROM entries WHERE active=1 AND json_extract(payload,'$.parent_workflow_run_id') IS NULL").fetchone()
            counts = {'total_active_count': active_count, 'total_active_root_count': root_counts[0], 'total_attention_root_count': root_counts[1], 'counts_complete': not pending and not incomplete}
            if pending and not active_only:
                counts.update(total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
                return {'items': [], 'next_cursor': None, 'has_more': False, 'bootstrap_pending': True, 'history_incomplete': incomplete, 'catalog_state': self.state(db), **({'scan_incomplete': False} if windowed else {}), **counts}
            if active_only:
                conditions.append('active=1')
                scoped.append('active=1')
            if boundary is not None:
                # A row-value bound seeks the (stamp, id) index on both columns, so many equal
                # stamps never widen the scanned range.
                conditions.append('(stamp,id) < (?,?)')
                values.extend(boundary)
                scoped.append('(stamp,id) < (?,?)')
                scoped_values.extend(boundary)
            window_end, rows_beyond, where_scope = None, False, ''
            if windowed:
                # The window is the next SCAN_ROWS rows in keyset order that satisfy the scope
                # (active_only, session, cursor) before any filter applies.
                where_scope = ' WHERE ' + ' AND '.join(scoped) if scoped else ''
                edge = db.execute('SELECT stamp,id FROM entries' + where_scope + ' ORDER BY stamp DESC,id DESC LIMIT 2 OFFSET ?', (*scoped_values, SCAN_ROWS - 1)).fetchall()
                if edge:
                    window_end, rows_beyond = edge[0], len(edge) > 1
            where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
            if windowed:
                # Isolate the candidate window first (scope and cursor bounds only), so no filter
                # can replace its index bounds; filters then apply to at most SCAN_ROWS rows.
                window = 'SELECT id AS wid,stamp AS wstamp FROM entries' + where_scope + ' ORDER BY stamp DESC,id DESC LIMIT ?'
                outer = ' WHERE ' + ' AND '.join(filter_conditions) if filter_conditions else ''
                rows = db.execute('SELECT id,stamp,payload FROM (' + window + ') JOIN entries ON entries.id=wid' + outer + ' ORDER BY wstamp DESC,wid DESC LIMIT ?',
                                  (*scoped_values, SCAN_ROWS, *filter_values, limit + 1)).fetchall()
            else:
                rows = db.execute('SELECT id,stamp,payload FROM entries' + where + ' ORDER BY stamp DESC,id DESC LIMIT ?', (*values, limit + 1)).fetchall()
            items, used, last = [], 512, None
            for identifier, stamp, payload in rows[:limit]:
                previous = last
                last = (identifier, stamp)
                if self._changed(db, identifier):
                    try:
                        value = self._load_bounded(identifier, loader)
                        if value is None:
                            self._remove(db, identifier)
                            continue
                        header, actual_stamp, active = value
                        self._put(db, header, actual_stamp, active, identifier)
                        if active_only and not active:
                            continue
                        match = db.execute('SELECT payload FROM entries WHERE id=?' + (' AND ' + ' AND '.join(conditions) if conditions else ''), (identifier, *values)).fetchone()
                        if match is None:
                            continue
                        payload = match[0]
                    except DeferredRead:
                        db.execute('INSERT OR IGNORE INTO pending VALUES (?)', (identifier,))
                        pending = True
                        last = previous
                        break
                size = len(payload.encode())
                if used + size > ROW_BYTES:
                    last = previous
                    break
                items.append(json.loads(payload))
                used += size
                last = (identifier, stamp)
            more = len(rows) > len(items)
            following = cursor if pending and more and last is None else None
            def encode(identifier, stamp):
                return base64.urlsafe_b64encode(json.dumps({'version': 1, 'scope': scope, 'id': identifier, 'stamp': stamp}, separators=(',', ':')).encode()).decode()
            if more and last is not None:
                following = encode(*last)
            scan_incomplete = False
            if windowed and not more and rows_beyond:
                # The window ended before the end of history: continue after the last examined row.
                # Only a page that did not fill is scan_incomplete; a full page just has more.
                scan_incomplete, more, following = len(items) < limit, True, encode(window_end[1], window_end[0])
            counts['total_active_count'] = None if windowed else db.execute('SELECT COUNT(*) FROM entries' + base + (' AND active=1' if count_values else ' WHERE active=1'), count_values).fetchone()[0]
            refreshed_roots = db.execute("SELECT COUNT(*),COALESCE(SUM(json_extract(payload,'$.status') IN ('needs_attention','needs_input')),0) FROM entries WHERE active=1 AND json_extract(payload,'$.parent_workflow_run_id') IS NULL").fetchone()
            counts.update(total_active_root_count=refreshed_roots[0], total_attention_root_count=refreshed_roots[1])
            if pending or incomplete:
                counts.update(counts_complete=False, total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
            return {'items': items, 'next_cursor': following, 'has_more': more, 'bootstrap_pending': pending, 'history_incomplete': incomplete, 'catalog_state': self.state(db), **({'scan_incomplete': scan_incomplete} if windowed else {}), **counts}
