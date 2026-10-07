"""Small durable listing index. Historical records are read, never migrated in place."""
from __future__ import annotations

import base64
import hashlib
import fcntl
import threading
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import wraps
import json
import logging
import os
import math
import sqlite3
from pathlib import Path
from typing import Any, Callable

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


_request_budget: ContextVar[MetadataBudget | None] = ContextVar('catalog_request_budget', default=None)


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


class DeferredRead(Exception):
    """The request budget is exhausted; leave this source queued for the next read."""


_locks: dict[str, tuple[threading.RLock, threading.local]] = {}
_locks_guard = threading.Lock()


@contextmanager
def catalog_lock(directory: Path):
    """Stable process/thread lock; nested projections retain the writer's outer lock."""
    directory.mkdir(parents=True, exist_ok=True)
    key = str(directory.resolve())
    with _locks_guard:
        mutex, local = _locks.setdefault(key, (threading.RLock(), threading.local()))
    with mutex:
        depth = getattr(local, 'depth', 0)
        if not depth:
            local.handle = (directory / '.listing.lock').open('a+b')
            fcntl.flock(local.handle.fileno(), fcntl.LOCK_EX)
        local.depth = depth + 1
        try:
            yield
        finally:
            local.depth -= 1
            if not local.depth:
                fcntl.flock(local.handle.fileno(), fcntl.LOCK_UN)
                local.handle.close()


class CatalogConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()

    def close(self):
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
    protected = {'task_id', 'workflow_run_id', 'parent_workflow_run_id', 'root_workflow_run_id', 'orchestrator_session_owner_run_id', 'workflow_session_owner_run_id', 'session_id', 'parent_task_id', 'root_task_id', 'spawned_by', 'status', 'started_at', 'created_at', 'updated_at', 'backend', 'kind', 'execution_contract', 'needs_direct_lookup', 'sessions'}
    protected.update({'persisted_status', 'observed_exit', 'process_identity_state', 'needs_reconciliation', 'status_reconciled'})
    for key, value in list(result.items()):
        if isinstance(value, str):
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
    return result


class Catalog:
    def __init__(self, directory: Path, suffix: str):
        self.directory, self.suffix = directory, suffix
        self._budget = _request_budget.get() or MetadataBudget()
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
            result['blocker_types'] = [row[0] for row in db.execute("SELECT DISTINCT COALESCE(json_extract(payload,'$.index_blocker'),'direct_inspection') FROM entries WHERE json_extract(payload,'$.needs_direct_lookup')=1 LIMIT 10")]
        if status != 'ready':
            result['reason'] = 'Metadata or caller identity requires direct inspection' if blocked else 'Preparing bounded metadata index'
        return result

    def ready(self) -> bool:
        return self.state()['status'] == 'ready'

    def invalidate(self, identifier: str) -> None:
        """Commit before replacing metadata, so a killed writer cannot leave ready authority."""
        with self.connect() as db:
            db.execute('INSERT OR IGNORE INTO pending VALUES (?)', (identifier,))
            db.execute("DELETE FROM state WHERE key='complete'")

    def _load_bounded(self, identifier: str, loader):
        path = self.directory / (identifier + self.suffix)
        try:
            initial_source = path.stat()
            size = initial_source.st_size
        except FileNotFoundError:
            return None
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
                value = loader(identifier, _metadata_budget=self)
            except ReadLimit:
                # The opened file may have grown since stat. Only a per-record breach
                # is persistent; aggregate exhaustion must be retried automatically.
                if path.stat().st_size > METADATA_BYTES:
                    return placeholder()
                raise DeferredRead() from None
        else:
            self.metadata_decodes += 1
            self.metadata_bytes += size
            value = loader(identifier)
        if source_identity(initial_source) != source_identity(path.stat()):
            raise DeferredRead()
        if value is not None:
            value[0]['_source_identity'] = source_identity(initial_source)
        return value

    def connect(self) -> sqlite3.Connection:
        lock = catalog_lock(self.directory)
        lock.__enter__()
        db = None
        try:
            db = self._connect_locked()
            db._catalog_lock = lock
            return db
        except BaseException:
            if db is not None:
                db.close()
            lock.__exit__(None, None, None)
            raise

    def _connect_locked(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.directory / '.listing.v7.sqlite3', timeout=10, factory=CatalogConnection)
        try:
            return self._initialize(db)
        except BaseException:
            db.close()
            raise

    def _initialize(self, db: sqlite3.Connection) -> sqlite3.Connection:
        db.execute('PRAGMA journal_mode=PERSIST')
        db.execute('CREATE TABLE IF NOT EXISTS entries (id TEXT PRIMARY KEY, stamp REAL NOT NULL, active INTEGER NOT NULL, session TEXT, payload TEXT NOT NULL)')
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
        db.execute('CREATE INDEX IF NOT EXISTS checkout_repo ON checkout_tasks(repo)')
        db.execute('CREATE INDEX IF NOT EXISTS checkout_run ON checkout_tasks(run_id)')
        version = db.execute("SELECT value FROM state WHERE key='schema_version'").fetchone()
        if version != ('7',):
            db.execute('DELETE FROM checkout_tasks')
            db.execute('DELETE FROM entries')
            db.execute('DELETE FROM callers')
            db.execute('DELETE FROM associations')
            db.execute('DROP TABLE sources')
            db.execute('CREATE TABLE sources (id TEXT PRIMARY KEY,mtime INTEGER,size INTEGER,inode INTEGER,device INTEGER)')
            db.execute('DELETE FROM pending')
            db.execute('DELETE FROM seen')
            db.execute('DELETE FROM state')
            db.execute("INSERT INTO state VALUES ('schema_version','7')")
        # Build only after invalidating old derivative headers. The partial index
        # makes warm authority readiness independent of retained history size.
        db.execute("CREATE INDEX IF NOT EXISTS incomplete_headers ON entries(id) WHERE json_extract(payload,'$.needs_direct_lookup')=1")
        db.execute("CREATE INDEX IF NOT EXISTS active_historical ON entries(id) WHERE active=1 AND COALESCE(json_extract(payload,'$.kind'),'workflow')!='builder' AND COALESCE(json_extract(payload,'$.execution_contract'),'')!='delegation'")
        db.commit()
        return db

    def _put(self, db: sqlite3.Connection, header: dict[str, Any], stamp: float, active: bool, identifier: str) -> None:
        header = dict(header)
        expected_source = header.pop('_source_identity', None)
        if expected_source is not None:
            try:
                if tuple(expected_source) != source_identity((self.directory / (identifier + self.suffix)).stat()):
                    raise DeferredRead()
            except FileNotFoundError:
                raise DeferredRead() from None
        checkout = header.pop('_checkout', [])
        db.execute('DELETE FROM checkout_tasks WHERE run_id=?', (identifier,))
        for task in checkout:
            encoded = json.dumps(task, ensure_ascii=True, separators=(',', ':'))
            if len(encoded.encode()) > 8 * 1024:
                header.update(needs_direct_lookup=True, status='unknown')
                continue
            db.execute('INSERT OR REPLACE INTO checkout_tasks VALUES (?,?,?,?)', (task['task_id'], identifier, task['repo_path'], encoded))
        caller = header.pop('_caller', None)
        associations = header.pop('_associations', {})
        db.execute("DELETE FROM associations WHERE json_extract(payload,'$.workflow_run_id')=?", (identifier,))
        for task_id, association in associations.items():
            db.execute('INSERT OR REPLACE INTO associations VALUES (?,?)', (task_id, json.dumps(association, separators=(',', ':'))))
        if caller is not None:
            # Identity must be lossless; display/content fields never enter the caller index.
            keys = {'task_id', 'backend', 'session_id', 'started_at', 'freedom', 'markers', 'pid', 'pgid', 'start_time', 'status', 'exit_code', 'root_task_id', 'parent_task_id', 'spawned_by', 'depth', 'max_depth', 'network'}
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
        session_id = header.get('session_id')
        header = bound_header(header)
        db.execute("INSERT INTO entries VALUES (?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET stamp=CASE WHEN json_extract(entries.payload,'$.needs_direct_lookup')=1 THEN excluded.stamp ELSE entries.stamp END,active=excluded.active,session=excluded.session,payload=excluded.payload",
                   (identifier, stamp, int(active), session_id, json.dumps(header, ensure_ascii=True, separators=(',', ':'))))
        generation = db.execute("SELECT value FROM state WHERE key='scan_generation'").fetchone()
        db.execute('INSERT OR REPLACE INTO seen VALUES (?,?)', (identifier, int(generation[0]) if generation else 0))
        try:
            source = (self.directory / f'{identifier}{self.suffix}').stat()
            db.execute('INSERT OR REPLACE INTO sources VALUES (?,?,?,?,?)', (identifier, *(tuple(expected_source) if expected_source is not None else source_identity(source))))
            db.execute('DELETE FROM pending WHERE id=?', (identifier,))
        except OSError:
            pass

    def _changed(self, db: sqlite3.Connection, identifier: str) -> bool:
        row = db.execute('SELECT mtime,size,inode,device FROM sources WHERE id=?', (identifier,)).fetchone()
        try:
            source = (self.directory / f'{identifier}{self.suffix}').stat()
            placeholder = db.execute("SELECT json_extract(payload,'$.needs_direct_lookup') FROM entries WHERE id=?", (identifier,)).fetchone()
            return row != (source.st_mtime_ns, source.st_size, getattr(source, 'st_ino', 0), getattr(source, 'st_dev', 0))
        except FileNotFoundError:
            return True

    def _remove(self, db: sqlite3.Connection, identifier: str) -> None:
        db.execute('DELETE FROM checkout_tasks WHERE run_id=?', (identifier,))
        db.execute("DELETE FROM associations WHERE json_extract(payload,'$.workflow_run_id')=?", (identifier,))
        for table in ('entries', 'callers', 'associations', 'sources', 'seen', 'pending'):
            db.execute(f'DELETE FROM {table} WHERE id=?', (identifier,))

    def _acknowledge_directory(self, db: sqlite3.Connection, previous_directory_mtime: int | None) -> None:
        # Older processes do not honor our lock. A matching-file replacement may
        # have overlapped this projection, so only discovery can acknowledge a
        # changed namespace. Keep the prior verified stamp and queued work.
        if dict(db.execute('SELECT key,value FROM state')).get('directory_mtime') != str(self.directory.stat().st_mtime_ns):
            db.execute("DELETE FROM state WHERE key='complete'")

    def record(self, header: dict[str, Any], stamp: float, active: bool, identifier: str, *, previous_directory_mtime: int | None = None) -> None:
        try:
            with self.connect() as db:
                self._put(db, header, stamp, active, identifier)
                self._acknowledge_directory(db, previous_directory_mtime)
        except (OSError, sqlite3.Error, DeferredRead):
            logging.getLogger(__name__).warning('Listing catalog update unavailable', exc_info=True)

    def remove(self, identifier: str, *, previous_directory_mtime: int | None = None) -> None:
        with self.connect() as db:
            self._remove(db, identifier)
            self._acknowledge_directory(db, previous_directory_mtime)

    def headers(self, identifiers: list[str], loader: Callable[[str], tuple[dict[str, Any], float, bool] | None]) -> list[dict[str, Any]]:
        if len(identifiers) > PAGE_LIMIT:
            raise ValueError('At most 100 identifiers may be requested')
        result = []
        with self.connect() as db:
            for identifier in dict.fromkeys(identifiers):
                row = db.execute('SELECT payload FROM entries WHERE id=?', (identifier,)).fetchone()
                if row is not None and not self._changed(db, identifier):
                    result.append(json.loads(row[0]))
                else:
                    try:
                        value = self._load_bounded(identifier, loader)
                    except DeferredRead:
                        db.execute('INSERT OR IGNORE INTO pending VALUES (?)', (identifier,))
                        key = 'task_id' if self.suffix == '.meta.json' else 'workflow_run_id'
                        result.append({key: identifier, 'status': 'unknown', 'indexing': True})
                        continue
                    if value is not None:
                        header, stamp, active = value
                        self._put(db, header, stamp, active, identifier)
                        result.append(bound_header({key: value for key, value in header.items() if not key.startswith('_')}))
                    else:
                        self._remove(db, identifier)
        return result

    def _discover(self, db: sqlite3.Connection) -> None:
        """Stream changed discovery into SQLite; unrelated churn never resets pending work."""
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
        stale = db.execute('SELECT id FROM sources WHERE id NOT IN (SELECT id FROM seen WHERE generation=?)', (generation,))
        while rows := stale.fetchmany(100):
            for (identifier,) in rows:
                self._remove(db, identifier)
        db.execute('DELETE FROM pending WHERE id NOT IN (SELECT id FROM seen WHERE generation=?)', (generation,))
        db.execute("INSERT OR REPLACE INTO state VALUES ('complete','1')")

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

    def page(self, loader: Callable[[str], tuple[dict[str, Any], float, bool] | None], *, limit: int = PAGE_LIMIT,
             cursor: str | None = None, active_only: bool = False, session_id: str | None = None) -> dict[str, Any]:
        if type(limit) is not int or not 1 <= limit <= PAGE_LIMIT:
            raise ValueError('limit must be 1–100')
        if not isinstance(active_only, bool) or session_id is not None and (not isinstance(session_id, str) or len(session_id) > 256):
            raise ValueError('active_only must be boolean; session_id must be a bounded string')
        scope = [self.identity, active_only, session_id]
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
            if session_id is not None:
                conditions.append('session=?')
                values.append(session_id)
            base = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
            active_count = db.execute('SELECT COUNT(*) FROM entries' + base + (' AND active=1' if conditions else ' WHERE active=1'), values).fetchone()[0]
            count_values = list(values)
            root_counts = db.execute("SELECT COUNT(*),COALESCE(SUM(json_extract(payload,'$.status') IN ('needs_attention','needs_input')),0) FROM entries WHERE active=1 AND json_extract(payload,'$.parent_workflow_run_id') IS NULL").fetchone()
            counts = {'total_active_count': active_count, 'total_active_root_count': root_counts[0], 'total_attention_root_count': root_counts[1], 'counts_complete': not pending and not incomplete}
            if pending and not active_only:
                counts.update(total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
                return {'items': [], 'next_cursor': None, 'has_more': False, 'bootstrap_pending': True, 'history_incomplete': incomplete, 'catalog_state': self.state(db), **counts}
            if active_only:
                conditions.append('active=1')
            if boundary is not None:
                conditions.append('(stamp < ? OR (stamp = ? AND id < ?))')
                values.extend((boundary[0], boundary[0], boundary[1]))
            where = ' WHERE ' + ' AND '.join(conditions) if conditions else ''
            rows = db.execute('SELECT id,stamp,payload FROM entries' + where + ' ORDER BY stamp DESC,id DESC LIMIT ?', (*values, limit + 1)).fetchall()
            items, used, last = [], 512, None
            for identifier, stamp, payload in rows[:limit]:
                previous = last
                last = (identifier, stamp)
                if self._changed(db, identifier) and self.metadata_decodes < BOOTSTRAP_LIMIT:
                    try:
                        value = self._load_bounded(identifier, loader)
                    except DeferredRead:
                        break
                    if value is None:
                        self._remove(db, identifier)
                        continue
                    header, actual_stamp, active = value
                    self._put(db, header, actual_stamp, active, identifier)
                    if active_only and not active:
                        continue
                    payload = db.execute('SELECT payload FROM entries WHERE id=?', (identifier,)).fetchone()[0]
                size = len(payload.encode())
                if used + size > ROW_BYTES:
                    last = previous
                    break
                items.append(json.loads(payload))
                used += size
                last = (identifier, stamp)
            more = len(rows) > len(items)
            following = None
            if more and last is not None:
                following = base64.urlsafe_b64encode(json.dumps({'version': 1, 'scope': scope, 'id': last[0], 'stamp': last[1]}, separators=(',', ':')).encode()).decode()
            counts['total_active_count'] = db.execute('SELECT COUNT(*) FROM entries' + base + (' AND active=1' if count_values else ' WHERE active=1'), count_values).fetchone()[0]
            refreshed_roots = db.execute("SELECT COUNT(*),COALESCE(SUM(json_extract(payload,'$.status') IN ('needs_attention','needs_input')),0) FROM entries WHERE active=1 AND json_extract(payload,'$.parent_workflow_run_id') IS NULL").fetchone()
            counts.update(total_active_root_count=refreshed_roots[0], total_attention_root_count=refreshed_roots[1])
            if pending or incomplete:
                counts.update(total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
            return {'items': items, 'next_cursor': following, 'has_more': more, 'bootstrap_pending': pending, 'history_incomplete': incomplete, 'catalog_state': self.state(db), **counts}
