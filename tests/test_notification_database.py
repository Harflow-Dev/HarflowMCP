"""Database identity, freshness, and refusal boundaries for notifications."""

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from harflow_mcp import notifications, workflow_notifications
from harflow_mcp import retention, store
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.state_ownership import state_session
from harflow_mcp.wire_contracts import PRODUCT_VERSION


@pytest.fixture
def logs(tmp_path, monkeypatch):
    root = tmp_path / "state"
    task_dir = root / "tasks"
    session = state_session(resolve_layout(root=root))
    session.__enter__()
    task_dir.mkdir(parents=True)
    monkeypatch.setenv(
        "HARFLOW_NOTIFY_DESTINATIONS",
        json.dumps({"hook": {"url": "https://example.test/hook"}}),
    )
    try:
        yield task_dir
    finally:
        session.__exit__(None, None, None)


def _database_path(logs: Path) -> Path:
    return logs.parent / "notifications.sqlite3"


def _create_database(path: Path, statements: list[str]) -> None:
    connection = sqlite3.connect(path)
    try:
        for statement in statements:
            connection.execute(statement)
        connection.commit()
    finally:
        connection.close()


def test_fresh_database_publishes_one_identity_and_all_tables(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)

    with sqlite3.connect(_database_path(logs)) as connection:
        identity = connection.execute(
            "SELECT singleton, product, format, schema_version, product_version "
            "FROM harflow_notification_identity"
        ).fetchall()
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        stored_type = connection.execute(
            "SELECT typeof(schema_version) FROM harflow_notification_identity"
        ).fetchone()[0]

    assert len(identity) == 1
    assert identity[0][:4] == (1, "harflow", "harflow.notifications", 1)
    assert isinstance(identity[0][3], int)
    assert stored_type == "integer"
    assert {
        "harflow_notification_identity",
        "subscriptions",
        "workflow_subscriptions",
        "workflow_deliveries",
    } <= tables


@pytest.mark.parametrize(
    "field,value",
    [
        ("product", "foreign"),
        ("format", "foreign.notifications"),
        ("schema_version", 2),
    ],
    ids=["foreign-product", "foreign-format", "unsupported-schema"],
)
def test_foreign_identity_is_refused_before_application_mutation(logs, field, value):
    path = _database_path(logs)
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    with sqlite3.connect(path) as connection:
        connection.execute(f"UPDATE harflow_notification_identity SET {field}=?", (value,))
        connection.commit()
    before = (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns)

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "missing")

    assert (path.read_bytes(), path.stat().st_mode, path.stat().st_mtime_ns) == before


def test_incomplete_schema_is_refused_instead_of_repaired(logs):
    path = _database_path(logs)
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE workflow_subscriptions")
        connection.commit()
    before = path.read_bytes()

    with pytest.raises(ValueError, match="notification"):
        workflow_notifications.status(logs, "run")

    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        names = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert "workflow_subscriptions" not in names


@pytest.mark.parametrize("alteration", ["foreign", "incomplete"])
def test_unsubscribe_refuses_foreign_or_incomplete_storage_before_rw_open(
    logs, monkeypatch, alteration
):
    from harflow_mcp import notification_database

    path = _database_path(logs)
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    if alteration == "foreign":
        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE harflow_notification_identity SET product='foreign'")
            connection.commit()
    else:
        with sqlite3.connect(path) as connection:
            connection.execute("DROP TABLE workflow_subscriptions")
            connection.commit()

    modes = []
    real_connect = notification_database._connect

    def recording_connect(*args, **kwargs):
        modes.append(args[1])
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(notification_database, "_connect", recording_connect)
    with pytest.raises(ValueError, match="notification"):
        notifications.unsubscribe(logs, "reserved")
    assert modes == ["ro"]


def test_requalification_after_preflight_blocks_rw_mutation(logs, monkeypatch):
    from harflow_mcp import notification_database

    path = _database_path(logs)
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    replacement = path.with_name("foreign-replacement.sqlite3")
    modes = []
    real_connect = notification_database._connect

    def replacing_connect(connect_path, mode, layout):
        modes.append(mode)
        db = real_connect(connect_path, mode, layout)
        if mode == "ro":
            _create_database(replacement, ["CREATE TABLE foreign_data (payload BLOB)"])
            with sqlite3.connect(replacement) as connection:
                connection.execute("INSERT INTO foreign_data VALUES ('keep')")
                connection.commit()
            replacement_bytes = replacement.read_bytes()
            replacement_mode = replacement.stat().st_mode
            replacement.replace(connect_path)
            replacing_connect.expected = (replacement_bytes, replacement_mode)
        return db

    monkeypatch.setattr(notification_database, "_connect", replacing_connect)
    with pytest.raises(ValueError, match="notification"):
        notifications.unsubscribe(logs, "reserved")

    assert modes == ["ro", "rw"]
    assert path.read_bytes() == replacing_connect.expected[0]
    assert path.stat().st_mode == replacing_connect.expected[1]
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT payload FROM foreign_data").fetchone()[0] == "keep"


def _sqlite_error(code):
    error = sqlite3.OperationalError("injected schema read failure")
    error.sqlite_errorcode = code
    return error


@pytest.mark.parametrize("failure_stage", ["tables", "qualification"])
def test_preflight_schema_errors_never_open_rw_before_positive_control(
    logs, monkeypatch, failure_stage
):
    from harflow_mcp import notification_database

    path = _database_path(logs)
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    modes = []
    real_connect = notification_database._connect

    def recording_connect(*args, **kwargs):
        modes.append(args[1])
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(notification_database, "_connect", recording_connect)
    if failure_stage == "tables":
        monkeypatch.setattr(
            notification_database, "_tables", lambda _db: (_ for _ in ()).throw(_sqlite_error(999))
        )
    else:
        monkeypatch.setattr(
            notification_database, "qualify", lambda _db, tables=None: (_ for _ in ()).throw(_sqlite_error(776))
        )

    with pytest.raises(sqlite3.OperationalError):
        notifications.unsubscribe(logs, "reserved")
    assert modes == ["ro"]

    monkeypatch.setattr(notification_database, "_tables", notification_database._tables)
    monkeypatch.undo()
    modes.clear()
    rows = notifications.unsubscribe(logs, "reserved")
    assert rows[0]["state"] == "unsubscribed"


def test_schema_version_storage_type_is_qualified_before_reads(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE harflow_notification_identity")
        connection.execute(
            "CREATE TABLE harflow_notification_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), product TEXT NOT NULL, format TEXT NOT NULL, schema_version NOT NULL, product_version TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO harflow_notification_identity VALUES (1, 'harflow', 'harflow.notifications', ?, ?)",
            ("1", PRODUCT_VERSION),
        )
        connection.commit()
        assert connection.execute(
            "SELECT typeof(schema_version) FROM harflow_notification_identity"
        ).fetchone()[0] == "text"
    before = path.read_bytes()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")

    assert path.read_bytes() == before


def test_identity_without_primary_key_is_refused_before_application_reads(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE harflow_notification_identity")
        connection.execute(
            "CREATE TABLE harflow_notification_identity (singleton INTEGER CHECK(singleton=1), product TEXT NOT NULL, format TEXT NOT NULL, schema_version NOT NULL, product_version TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO harflow_notification_identity VALUES (1, 'harflow', 'harflow.notifications', 1, ?)",
            (PRODUCT_VERSION,),
        )
        connection.commit()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")


def test_zero_identity_rows_are_refused_before_application_reads(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DELETE FROM harflow_notification_identity")
        connection.commit()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")


def test_identity_singleton_must_be_a_primary_key(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE harflow_notification_identity")
        connection.execute(
            "CREATE TABLE harflow_notification_identity (singleton INTEGER CHECK(singleton=1), product TEXT NOT NULL, format TEXT NOT NULL, schema_version NOT NULL, product_version TEXT NOT NULL)"
        )
        connection.execute(
            "INSERT INTO harflow_notification_identity VALUES (1, 'harflow', 'harflow.notifications', 1, ?)",
            (PRODUCT_VERSION,),
        )
        connection.commit()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")


def test_required_subscription_columns_are_not_repaired(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE subscriptions")
        connection.execute("CREATE TABLE subscriptions (id TEXT PRIMARY KEY, task_id TEXT NOT NULL)")
        connection.commit()
    before = path.read_bytes()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")

    assert path.read_bytes() == before


_SUBSCRIPTION_COLUMNS = """(
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, destination TEXT NOT NULL,
    state TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL,
    attempts INTEGER NOT NULL DEFAULT {attempts_default}, due REAL NOT NULL DEFAULT 0,
    lease TEXT, lease_until REAL NOT NULL DEFAULT 0, payload BLOB, error TEXT,
    UNIQUE(task_id,destination)
)"""


@pytest.mark.parametrize(
    "variant",
    [
        "attempts TEXT NOT NULL DEFAULT 0",
        "attempts INTEGER DEFAULT 0",
        "attempts INTEGER NOT NULL DEFAULT 1",
    ],
    ids=["wrong-affinity", "nullable-essential-column", "wrong-default"],
)
def test_essential_column_contract_is_checked_causally(logs, variant):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    ddl = _SUBSCRIPTION_COLUMNS.format(attempts_default="0").replace(
        "attempts INTEGER NOT NULL DEFAULT 0", variant
    )
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE subscriptions")
        connection.execute("CREATE TABLE subscriptions " + ddl)
        connection.commit()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")


@pytest.mark.parametrize(
    "index_sql",
    [
        "CREATE UNIQUE INDEX subscriptions_partial ON subscriptions(task_id,destination) WHERE state='pending'",
        "CREATE UNIQUE INDEX subscriptions_expression ON subscriptions(lower(task_id),destination)",
        "CREATE UNIQUE INDEX subscriptions_extra_key ON subscriptions(task_id,destination,state)",
        "CREATE UNIQUE INDEX subscriptions_nocase ON subscriptions(task_id COLLATE NOCASE,destination)",
    ],
    ids=["partial", "expression", "extra-key", "nocase"],
)
def test_compound_uniqueness_requires_exact_unconditional_binary_key(logs, index_sql):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE subscriptions")
        connection.execute(
            "CREATE TABLE subscriptions (id TEXT PRIMARY KEY, task_id TEXT NOT NULL, destination TEXT NOT NULL, state TEXT NOT NULL, created REAL NOT NULL, updated REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL DEFAULT 0, lease TEXT, lease_until REAL NOT NULL DEFAULT 0, payload BLOB, error TEXT)"
        )
        connection.execute(index_sql)
        connection.commit()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")


def test_valid_schema_is_a_positive_control_for_constraint_checks(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    assert notifications.status(logs, "reserved")[0]["state"] == "pending"


def test_harmless_additive_column_and_index_are_preserved(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE subscriptions ADD COLUMN note TEXT")
        connection.execute("CREATE INDEX subscriptions_note ON subscriptions(note)")
        connection.commit()
    assert notifications.status(logs, "reserved")[0]["state"] == "pending"


def test_required_additive_column_without_default_is_refused(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE subscriptions")
        ddl = _SUBSCRIPTION_COLUMNS.format(attempts_default="0").replace(
            "UNIQUE(task_id,destination)", "required_note TEXT NOT NULL, UNIQUE(task_id,destination)"
        )
        connection.execute("CREATE TABLE subscriptions " + ddl)
        connection.commit()
    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")


@pytest.mark.parametrize("product_version", [f"{PRODUCT_VERSION.rsplit('.', 1)[0]}.999", "invalid", "999.0.0"])
def test_product_version_compatibility_is_qualified_and_preserved(logs, product_version):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE harflow_notification_identity SET product_version=?", (product_version,))
        connection.commit()
    if product_version.endswith(".999"):
        assert notifications.status(logs, "reserved")[0]["state"] == "pending"
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT product_version FROM harflow_notification_identity").fetchone()[0] == product_version
    else:
        with pytest.raises(ValueError, match="notification"):
            notifications.status(logs, "reserved")


def test_zero_byte_existing_only_status_does_not_connect_or_initialize(logs):
    path = _database_path(logs)
    path.touch()
    before_mode = path.stat().st_mode

    assert notifications.status(logs, "missing") == []
    assert workflow_notifications.status(logs, "missing") == {
        "subscriptions": [],
        "deliveries": [],
    }
    assert path.read_bytes() == b""
    assert path.stat().st_mode == before_mode


@pytest.mark.parametrize("operation", ["status", "workflow_status", "unsubscribe", "pinned_tasks"])
def test_existing_only_empty_database_paths_do_not_open_sqlite(logs, monkeypatch, operation):
    from harflow_mcp import notification_database

    path = _database_path(logs)
    path.touch()
    monkeypatch.setattr(
        notification_database,
        "_connect",
        lambda *args, **kwargs: pytest.fail("empty existing-only path was opened"),
    )

    if operation == "status":
        assert notifications.status(logs, "missing") == []
    elif operation == "workflow_status":
        assert workflow_notifications.status(logs, "missing") == {
            "subscriptions": [],
            "deliveries": [],
        }
    elif operation == "unsubscribe":
        assert notifications.unsubscribe(logs, "missing") == []
    else:
        assert notifications.pinned_tasks(logs) == set()

    assert path.read_bytes() == b""


def test_sidecar_presence_is_refused_before_sqlite_open(logs, monkeypatch):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    sidecar = Path(str(path) + "-wal")
    sidecar.write_bytes(b"foreign wal bytes")
    before = sidecar.read_bytes()
    from harflow_mcp import notification_database

    monkeypatch.setattr(
        notification_database,
        "_connect",
        lambda *args, **kwargs: pytest.fail("WAL sidecar was opened"),
    )
    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")
    assert sidecar.read_bytes() == before


def test_shm_sidecar_is_refused_before_sqlite_open(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    sidecar = Path(str(path) + "-shm")
    sidecar.write_bytes(b"foreign shm bytes")
    from harflow_mcp import notification_database
    original_connect = notification_database._connect
    notification_database._connect = lambda *args, **kwargs: pytest.fail("SHM sidecar was opened")
    try:
        with pytest.raises(ValueError, match="notification"):
            notifications.status(logs, "reserved")
    finally:
        notification_database._connect = original_connect
    assert sidecar.read_bytes() == b"foreign shm bytes"


def test_wal_header_is_refused_before_sqlite_open(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    raw = bytearray(path.read_bytes())
    raw[18] = 2
    path.write_bytes(raw)
    from harflow_mcp import notification_database
    original_connect = notification_database._connect
    notification_database._connect = lambda *args, **kwargs: pytest.fail("WAL header was opened")
    try:
        with pytest.raises(ValueError, match="notification"):
            notifications.status(logs, "reserved")
    finally:
        notification_database._connect = original_connect


def test_regular_leftover_journal_does_not_block_fresh_bootstrap(logs):
    path = _database_path(logs)
    path.touch()
    journal = Path(str(path) + "-journal")
    journal.write_bytes(b"leftover regular journal")

    notifications.subscribe(logs, "reserved", "hook", reservation=True)

    assert notifications.status(logs, "reserved")[0]["state"] == "pending"


def test_symlinked_database_is_refused_before_sqlite_open(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    target = path.with_name("real-notifications.sqlite3")
    path.replace(target)
    path.symlink_to(target)

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "reserved")
    assert target.exists()


def test_bootstrap_failure_rolls_back_identity_and_tables(logs, monkeypatch):
    from harflow_mcp import notification_database

    def fail_workflow_tables(db):
        raise RuntimeError("workflow schema fault")

    monkeypatch.setattr(notification_database, "create_workflow_tables", fail_workflow_tables)
    with pytest.raises(RuntimeError, match="workflow schema fault"):
        notifications.subscribe(logs, "reserved", "hook", reservation=True)

    path = _database_path(logs)
    assert path.exists()
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0] == 0


def test_concurrent_first_initializers_converge_on_one_qualified_database(logs):
    from concurrent.futures import ThreadPoolExecutor

    def initialize(task_id):
        return notifications.subscribe(logs, task_id, "hook", reservation=True)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(initialize, ["first", "second"]))

    assert {result["task_id"] for result in results} == {"first", "second"}
    with sqlite3.connect(_database_path(logs)) as connection:
        assert connection.execute(
            "SELECT count(*) FROM harflow_notification_identity"
        ).fetchone()[0] == 1


def _leave_hot_rollback_journal(path: Path, sql: str) -> None:
    script = (
        "import os, sqlite3, sys, time\n"
        "path, sql = sys.argv[1:]\n"
        "db = sqlite3.connect(path, timeout=2)\n"
        "db.execute('PRAGMA journal_mode=DELETE')\n"
        "db.execute('PRAGMA synchronous=FULL')\n"
        "db.execute('PRAGMA cache_size=8')\n"
        "db.execute('BEGIN IMMEDIATE')\n"
        "db.execute(sql)\n"
        "os._exit(0)\n"
    )
    result = subprocess.run([sys.executable, "-I", "-c", script, str(path), sql], timeout=5)
    assert result.returncode == 0
    assert Path(str(path) + "-journal").exists()


def test_qualified_hot_rollback_journal_recovers_on_writable_unsubscribe(logs):
    notifications.subscribe(logs, "reserved", "hook", reservation=True)
    path = _database_path(logs)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE hot_payload (id INTEGER PRIMARY KEY, payload BLOB NOT NULL)")
        connection.executemany(
            "INSERT INTO hot_payload VALUES (?, ?)",
            [(index, b"A" * 4096) for index in range(1, 257)],
        )
        connection.commit()
    mode = path.stat().st_mode
    _leave_hot_rollback_journal(
        path,
        "UPDATE hot_payload SET payload=zeroblob(4096)",
    )
    with pytest.raises(sqlite3.OperationalError) as read_error:
        notifications.status(logs, "reserved")
    assert read_error.value.sqlite_errorcode == 776

    rows = notifications.unsubscribe(logs, "reserved")
    assert rows[0]["state"] == "unsubscribed"
    assert not Path(str(path) + "-journal").exists()
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT count(*), sum(length(payload)), sum(substr(payload,1,1)=X'41') FROM hot_payload"
        ).fetchone() == (256, 256 * 4096, 256)
    assert path.stat().st_mode == mode


def test_foreign_hot_rollback_journal_recovers_then_refuses_without_adoption(logs):
    path = _database_path(logs)
    _create_database(path, ["CREATE TABLE foreign_data (id INTEGER PRIMARY KEY, marker TEXT, payload BLOB)"])
    with sqlite3.connect(path) as connection:
        connection.executemany(
            "INSERT INTO foreign_data VALUES (?, ?, ?)",
            [(index, "committed", b"A" * 4096) for index in range(1, 257)],
        )
        connection.commit()
    mode = path.stat().st_mode
    _leave_hot_rollback_journal(
        path,
        "UPDATE foreign_data SET marker='uncommitted', payload=zeroblob(4096)",
    )

    with pytest.raises(sqlite3.OperationalError) as read_error:
        notifications.status(logs, "missing")
    assert read_error.value.sqlite_errorcode == 776

    with pytest.raises(ValueError, match="notification"):
        notifications.unsubscribe(logs, "missing")

    assert not Path(str(path) + "-journal").exists()

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT count(*), sum(length(payload)), sum(marker='committed') FROM foreign_data"
        ).fetchone() == (256, 256 * 4096, 256)
        assert connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE name='harflow_notification_identity'"
        ).fetchone()[0] == 0
    assert path.stat().st_mode == mode


def test_worker_survives_notification_database_refusal_without_delivery(logs, monkeypatch):
    import asyncio

    path = _database_path(logs)
    _create_database(path, ["CREATE TABLE foreign_data (payload BLOB)"])
    observed = []
    async def sleep_once(_delay):
        observed.append(True)
        if len(observed) >= 2:
            raise asyncio.CancelledError
    monkeypatch.setattr(notifications.asyncio, "sleep", sleep_once)
    monkeypatch.setattr(notifications, "_deliver", lambda *args: pytest.fail("invalid DB reached HTTP delivery"))

    async def run_once():
        with pytest.raises(asyncio.CancelledError):
            await notifications.worker(logs)

    asyncio.run(run_once())
    assert observed == [True, True]


def test_retention_skips_refused_notification_database_without_deletion(logs, monkeypatch, caplog):
    from datetime import datetime, timezone

    monkeypatch.setenv("HARFLOW_RETENTION_DAYS", "30")
    old = "2020-01-01T00:00:00+00:00"
    deletable = store.TaskRecord(
        task_id="a" * 32,
        backend="codex",
        session_id=None,
        repo_path="/tmp/repo",
        started_at=old,
        status="completed",
        exit_code=0,
        finished_at=old,
    )
    store.write(logs, deletable)
    assert retention.sweep(logs, 30, datetime.now(timezone.utc))["deleted_tasks"] == 1

    kept = store.TaskRecord(
        task_id="b" * 32,
        backend="codex",
        session_id=None,
        repo_path="/tmp/repo",
        started_at=old,
        status="completed",
        exit_code=0,
        finished_at=old,
    )
    store.write(logs, kept)
    notifications.subscribe(logs, kept.task_id, "hook", reservation=True)
    path = _database_path(logs)
    path.unlink()
    _create_database(path, ["CREATE TABLE foreign_data (payload BLOB)"])
    with pytest.raises(ValueError, match="notification"):
        retention.sweep(logs, 30, datetime.now(timezone.utc))
    result = retention.maybe_sweep(logs, now=datetime.now(timezone.utc))

    assert result is None
    assert store.read(logs, kept.task_id) is not None
    assert "retention sweep failed" in caplog.text


def test_unrelated_sqlite_database_is_refused_without_adoption(logs):
    path = _database_path(logs)
    _create_database(path, ["CREATE TABLE foreign_data (payload BLOB)"])
    with sqlite3.connect(path) as connection:
        connection.execute("INSERT INTO foreign_data VALUES (?)", (b"keep",))
        connection.commit()
    before = path.read_bytes()

    with pytest.raises(ValueError, match="notification"):
        notifications.status(logs, "missing")

    assert path.read_bytes() == before
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT payload FROM foreign_data").fetchone()[0] == b"keep"
