"""Qualification and atomic bootstrap for the shared notification database."""
from __future__ import annotations

import os
import re
import sqlite3
import stat
from pathlib import Path

from .state_access import (OwnedConnection, assert_task_owned, bind_connection,
                           open_descriptor, task_layout)
from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION

IDENTITY_TABLE = 'harflow_notification_identity'
FORMAT = 'harflow.notifications'
SCHEMA_VERSION = 1


class NotificationDatabaseError(ValueError):
    """Existing notification storage does not satisfy the Harflow contract."""


def _refuse():
    raise NotificationDatabaseError('Harflow notification database is incompatible')


# name: (declared affinity, not-null, primary-key position, default)
_IDENTITY = {
    'singleton': ('INTEGER', 0, 1, None), 'product': ('TEXT', 1, 0, None),
    'format': ('TEXT', 1, 0, None), 'schema_version': ('BLOB', 1, 0, None),
    'product_version': ('TEXT', 1, 0, None),
}
_SUBSCRIPTIONS = {
    'id': ('TEXT', 0, 1, None),
    **{name: ('TEXT', 1, 0, None) for name in ('task_id', 'destination', 'state')},
    **{name: ('REAL', 1, 0, None) for name in ('created', 'updated')},
    'attempts': ('INTEGER', 1, 0, '0'), 'due': ('REAL', 1, 0, '0'),
    'lease': ('TEXT', 0, 0, None), 'lease_until': ('REAL', 1, 0, '0'),
    'payload': ('BLOB', 0, 0, None), 'error': ('TEXT', 0, 0, None),
}
_WORKFLOW_SUBSCRIPTIONS = {
    'id': ('TEXT', 0, 1, None),
    **{name: ('TEXT', 1, 0, None) for name in ('workflow_run_id', 'destination', 'events', 'state')},
    **{name: ('INTEGER', 1, 0, None) for name in ('from_sequence', 'last_point_sequence')},
    **{name: ('INTEGER', 0, 0, None) for name in ('run_mtime_ns', 'run_size')},
    **{name: ('REAL', 1, 0, None) for name in ('created', 'updated')},
}
_WORKFLOW_DELIVERIES = {
    'id': ('TEXT', 0, 1, None),
    **{name: ('TEXT', 1, 0, None) for name in ('subscription_id', 'event_key', 'event', 'state')},
    'attempts': ('INTEGER', 1, 0, '0'), 'due': ('REAL', 1, 0, '0'),
    'lease': ('TEXT', 0, 0, None), 'lease_until': ('REAL', 1, 0, '0'),
    'payload': ('BLOB', 1, 0, None), 'error': ('TEXT', 0, 0, None),
    **{name: ('REAL', 1, 0, None) for name in ('created', 'updated')},
}
_SUPERVISION_DELIVERIES = {
    **_WORKFLOW_DELIVERIES,
    'subscription_kind': ('TEXT', 1, 0, None),
    'task_id': ('TEXT', 1, 0, None),
    'destination': ('TEXT', 1, 0, None),
}
_SUPERVISION_DDL = """CREATE TABLE supervision_deliveries (
    id TEXT PRIMARY KEY, subscription_id TEXT NOT NULL, subscription_kind TEXT NOT NULL,
    task_id TEXT NOT NULL, destination TEXT NOT NULL, event_key TEXT NOT NULL,
    event TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
    due REAL NOT NULL DEFAULT 0, lease TEXT, lease_until REAL NOT NULL DEFAULT 0,
    payload BLOB NOT NULL, error TEXT, created REAL NOT NULL, updated REAL NOT NULL,
    UNIQUE(subscription_kind,subscription_id,event_key))"""
_SCHEMAS = {
    IDENTITY_TABLE: (_IDENTITY, None),
    'subscriptions': (_SUBSCRIPTIONS, ('task_id', 'destination')),
    'workflow_subscriptions': (_WORKFLOW_SUBSCRIPTIONS, ('workflow_run_id', 'destination')),
    'workflow_deliveries': (_WORKFLOW_DELIVERIES, ('subscription_id', 'event_key')),
}
_WORKFLOW_DDL = (
    '''CREATE TABLE workflow_subscriptions (
        id TEXT PRIMARY KEY, workflow_run_id TEXT NOT NULL, destination TEXT NOT NULL,
        events TEXT NOT NULL, state TEXT NOT NULL, from_sequence INTEGER NOT NULL,
        last_point_sequence INTEGER NOT NULL, run_mtime_ns INTEGER, run_size INTEGER,
        created REAL NOT NULL, updated REAL NOT NULL, UNIQUE(workflow_run_id,destination))''',
    '''CREATE TABLE workflow_deliveries (
        id TEXT PRIMARY KEY, subscription_id TEXT NOT NULL, event_key TEXT NOT NULL,
        event TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        due REAL NOT NULL DEFAULT 0, lease TEXT, lease_until REAL NOT NULL DEFAULT 0,
        payload BLOB NOT NULL, error TEXT, created REAL NOT NULL, updated REAL NOT NULL,
        UNIQUE(subscription_id,event_key))''',
)


def _quote(value):
    return '"' + value.replace('"', '""') + '"'


def _affinity(declared):
    declared = declared.upper()
    if 'INT' in declared:
        return 'INTEGER'
    if any(token in declared for token in ('CHAR', 'CLOB', 'TEXT')):
        return 'TEXT'
    if not declared or 'BLOB' in declared:
        return 'BLOB'
    if any(token in declared for token in ('REAL', 'FLOA', 'DOUB')):
        return 'REAL'
    return 'NUMERIC'


def _default(value):
    if value is None:
        return None
    value = value.strip()
    while value.startswith('(') and value.endswith(')'):
        value = value[1:-1].strip()
    return value


def _insert_default(value):
    """Recognize non-null SQL literals without executing stored expressions."""
    value = _default(value)
    return value is not None and re.fullmatch(
        r"(?:[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?|'(?:[^']|'')*'|[xX]'(?:[0-9a-fA-F]{2})*'|TRUE|FALSE)",
        value, re.IGNORECASE) is not None


def _unique(db, table, columns, *, primary=False):
    for index in db.execute('PRAGMA index_list(' + _quote(table) + ')'):
        if not index[2] or index[4] or (primary and index[3] != 'pk'):
            continue
        keys = [row for row in db.execute('PRAGMA index_xinfo(' + _quote(index[1]) + ')') if row[5]]
        if (len(keys) == len(columns) and {row[2] for row in keys} == set(columns)
                and all(row[1] >= 0 and row[4] == 'BINARY' for row in keys)):
            return True
    return False


def _tables(db):
    return {row[0]: row[1] for row in db.execute("SELECT name,sql FROM sqlite_schema WHERE type='table'")}


def qualify(db, tables=None):
    """Read identity and essential schema without repairing existing storage."""
    if tables is None:
        tables = _tables(db)
    schemas = dict(_SCHEMAS)
    if 'supervision_deliveries' in tables:
        schemas['supervision_deliveries'] = (_SUPERVISION_DELIVERIES,
                                            ('subscription_kind', 'subscription_id', 'event_key'))
    for table, (expected, unique) in schemas.items():
        if table not in tables:
            _refuse()
        columns = list(db.execute('PRAGMA table_xinfo(' + _quote(table) + ')'))
        actual = {row[1]: row for row in columns}
        for name, definition in expected.items():
            row = actual.get(name)
            if row is None or row[6] or (_affinity(row[2]), row[3], row[5], _default(row[4])) != definition:
                _refuse()
        # Additional required insert values would break the existing partial INSERTs.
        if any(row[1] not in expected and not row[6] and (row[5] or (row[3] and not _insert_default(row[4]))) for row in columns):
            _refuse()
        if table == IDENTITY_TABLE:
            if actual['singleton'][2].upper() != 'INTEGER':
                _refuse()
        elif not _unique(db, table, ('id',), primary=True):
            _refuse()
        if unique is not None and not _unique(db, table, unique):
            _refuse()
    rows = db.execute('SELECT singleton,product,format,schema_version,product_version,typeof(schema_version) FROM ' + IDENTITY_TABLE).fetchall()
    if len(rows) != 1:
        _refuse()
    row = rows[0]
    if (type(row[0]) is not int or row[0] != 1 or row[1] != PRODUCT or row[2] != FORMAT
            or type(row[3]) is not int or row[3] != SCHEMA_VERSION or row[5] != 'integer'):
        _refuse()
    try:
        compatible = persisted_compatible(row[4], FORMAT, SCHEMA_VERSION)
    except ValueError:
        compatible = False
    if not compatible:
        _refuse()


def _inspect(path, layout):
    """Return missing/zero admission after no-follow main and sidecar checks."""
    for suffix in ('-wal', '-shm', '-journal'):
        sidecar = Path(str(path) + suffix)
        try:
            info = sidecar.lstat()
        except FileNotFoundError:
            continue
        if suffix != '-journal' or not stat.S_ISREG(info.st_mode):
            _refuse()
    try:
        info = path.lstat()
    except FileNotFoundError:
        return 'missing'
    if not stat.S_ISREG(info.st_mode):
        _refuse()
    fd = open_descriptor(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0), layout=layout)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            _refuse()
        header = os.read(fd, 100)
        if header[:16] == b'SQLite format 3\x00' and len(header) >= 20 and (header[18] == 2 or header[19] == 2):
            _refuse()
        return 'zero' if info.st_size == 0 else 'existing'
    finally:
        fd.close()


def _connect(path, mode, layout):
    db = bind_connection(sqlite3.connect(path.absolute().as_uri() + '?mode=' + mode,
                                        uri=True, timeout=2, factory=OwnedConnection), layout)
    db.row_factory = sqlite3.Row
    return db


def create_workflow_tables(db):
    """Internal creator; only the atomic fresh bootstrap authorizes this DDL."""
    if not isinstance(db, OwnedConnection) or not getattr(db, '_notification_bootstrap', False) or not db.in_transaction:
        _refuse()
    db._assert()
    for statement in _WORKFLOW_DDL:
        db.execute(statement)


def _bootstrap(db):
    if type(SCHEMA_VERSION) is not int or SCHEMA_VERSION != 1 or not persisted_compatible(PRODUCT_VERSION, FORMAT, SCHEMA_VERSION):
        _refuse()
    db.execute('BEGIN IMMEDIATE')
    try:
        schema = db.execute('SELECT name FROM sqlite_schema').fetchall()
        if schema:
            qualify(db)  # A concurrent initializer may already have committed.
        else:
            if db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal':
                _refuse()
            if db.execute('PRAGMA user_version').fetchone()[0] or db.execute('PRAGMA application_id').fetchone()[0]:
                _refuse()
            db.execute('''CREATE TABLE harflow_notification_identity (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), product TEXT NOT NULL,
                format TEXT NOT NULL, schema_version NOT NULL, product_version TEXT NOT NULL)''')
            db.execute('INSERT INTO harflow_notification_identity VALUES(?,?,?,?,?)',
                       (1, PRODUCT, FORMAT, SCHEMA_VERSION, PRODUCT_VERSION))
            db.execute('''CREATE TABLE subscriptions (
                id TEXT PRIMARY KEY, task_id TEXT NOT NULL, destination TEXT NOT NULL,
                state TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL DEFAULT 0,
                lease TEXT, lease_until REAL NOT NULL DEFAULT 0, payload BLOB, error TEXT,
                UNIQUE(task_id,destination))''')
            db._notification_bootstrap = True
            try:
                from .workflow_notifications import initialize
                initialize(db)
            finally:
                db._notification_bootstrap = False
            qualify(db)
        db.commit()
    except BaseException:
        db.rollback()
        raise


def _extend(db):
    """Add the optional delivery ledger only after qualifying the original schema."""
    db.execute('BEGIN IMMEDIATE')
    try:
        qualify(db)
        if 'supervision_deliveries' not in _tables(db):
            db.execute(_SUPERVISION_DDL)
        qualify(db)
        db.commit()
    except BaseException:
        db.rollback()
        raise


def open_database(log_dir, *, create, mode):
    """Open qualified storage; native rollback is permitted only on writable paths."""
    if mode not in ('ro', 'rw') or (create and mode != 'rw'):
        raise ValueError('Invalid notification database access mode')
    assert_task_owned(log_dir)
    layout = task_layout(log_dir)
    path = Path(log_dir).parent / 'notifications.sqlite3'
    admission = _inspect(path, layout)
    if not create and admission in ('missing', 'zero'):
        return None
    fresh = create and admission in ('missing', 'zero')
    if not fresh:
        try:
            preflight = _connect(path, 'ro', layout)
        except sqlite3.OperationalError:
            if not create and _inspect(path, layout) == 'missing':
                return None
            raise
        try:
            try:
                tables = _tables(preflight)
            except sqlite3.OperationalError as exc:
                if mode != 'rw' or getattr(exc, 'sqlite_errorcode', None) != 776:
                    raise
                # Only the first schema read can authorize native rollback below.
            else:
                qualify(preflight, tables)
                if mode == 'ro':
                    return preflight
        except BaseException:
            preflight.close()
            raise
        preflight.close()
    # Recheck WAL and regular-file signs before opening the writable connection.
    current = _inspect(path, layout)
    if not fresh and current == 'missing':
        if not create:
            return None
        raise FileNotFoundError(path)
    if fresh:
        fd = open_descriptor(path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), layout=layout)
        fd.close()
    try:
        db = _connect(path, 'rw', layout)
    except sqlite3.OperationalError:
        if not create and _inspect(path, layout) == 'missing':
            return None
        raise
    try:
        if fresh:
            _bootstrap(db)
        else:
            qualify(db)  # First rw query may perform SQLite's native hot-journal rollback.
        if db.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal':
            _refuse()
        _extend(db)
        if create:
            db.execute('PRAGMA journal_mode=DELETE')
            fd = open_descriptor(path, os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), layout=layout)
            try:
                os.fchmod(fd, 0o600)
            finally:
                fd.close()
        return db
    except BaseException:
        db.close()
        raise
