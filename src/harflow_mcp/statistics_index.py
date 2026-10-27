"""Owned, bounded foreground preparation of a disposable statistics source projection."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import time

from .catalog import METADATA_BYTES, MetadataBudget
from .state_access import OwnedConnection, bind_connection
from .state_ownership import assert_owned

RAW_ENTRY_LIMIT = 100000
SOURCE_LIMIT = 10000
DECODE_LIMIT = 100
DEADLINE_SECONDS = 2.0
INDEX_FILENAME = ".statistics.v1.sqlite3"
INDEX_BYTES = 64 * 1024 * 1024
SOURCE_BYTES = 128 * 1024
SCHEMA_VERSION = 1
IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")


class IndexLimit(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__(message)


def witness(source):
    return [source.st_dev, source.st_ino, source.st_mtime_ns, source.st_ctime_ns, source.st_size]


def check_deadline(deadline):
    if time.monotonic() >= deadline:
        raise IndexLimit("discovery_budget", "Statistics foreground deadline exhausted")


def diagnostic(code, message):
    return {"code": code, "message": str(message)[:512]}


class StatisticsIndex:
    def __init__(self, layout, *, deadline=None):
        self.layout = layout
        self.deadline = deadline if deadline is not None else time.monotonic() + DEADLINE_SECONDS
        self.budget = MetadataBudget()
        self.families = {"task": (layout.tasks_directory, ".meta.json"),
                         "run": (layout.root / "workflow-runs", ".json"),
                         "owner": (layout.root / "workflow-owners", ".json")}

    def path(self, row):
        directory, suffix = self.families[row["kind"]]
        return directory / (row["id"] + suffix)

    def discover(self):
        """Scan only exact source families; raw names and candidate documents have separate caps."""
        assert_owned(self.layout)
        found, entries_seen = [], 0
        for kind, (directory, suffix) in self.families.items():
            check_deadline(self.deadline)
            try:
                source = directory.lstat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(source.st_mode):
                raise IndexLimit("source_unavailable", "Statistics source directory is not a real directory")
            with os.scandir(directory) as entries:
                for entry in entries:
                    check_deadline(self.deadline)
                    entries_seen += 1
                    if entries_seen > RAW_ENTRY_LIMIT:
                        raise IndexLimit("history_limit", "Raw history name limit exceeded")
                    if not entry.name.endswith(suffix):
                        continue
                    identifier = entry.name[:-len(suffix)]
                    if not IDENTIFIER.fullmatch(identifier):
                        continue
                    if len(found) >= SOURCE_LIMIT:
                        raise IndexLimit("history_limit", "Statistics source history limit exceeded")
                    try:
                        source = entry.stat(follow_symlinks=False)
                        identity = witness(source)
                    except OSError:
                        identity = None
                    found.append({"key": kind + ":" + identifier, "kind": kind,
                                  "id": identifier, "witness": identity})
        return sorted(found, key=lambda r: r["key"])

    @contextmanager
    def connect(self):
        """Serialize derivative schema repair and transactions; never modify source documents."""
        assert_owned(self.layout)
        lock_path = self.layout.root / ".statistics.v1.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        db = None
        try:
            while True:
                check_deadline(self.deadline)
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    time.sleep(0.005)
            path = self.layout.root / INDEX_FILENAME
            if path.is_symlink():
                raise IndexLimit("index_unavailable", "Statistics derivative path is a symbolic link")
            try:
                db = self._open(path)
                version = db.execute("PRAGMA user_version").fetchone()[0]
                if version not in (0, SCHEMA_VERSION):
                    raise sqlite3.DatabaseError("Unknown derivative version")
                self._schema(db)
            except sqlite3.DatabaseError:
                if db is not None:
                    db.close()
                assert_owned(self.layout)
                for suffix in ("", "-journal", "-wal", "-shm"):
                    path.with_name(path.name + suffix).unlink(missing_ok=True)
                db = self._open(path)
                self._schema(db)
            check_deadline(self.deadline)
            db.set_progress_handler(lambda: int(time.monotonic() >= self.deadline), 1000)
            db.execute("BEGIN IMMEDIATE")
            yield db
            # Completed batches survive deadline exhaustion. SQLite has a 50ms
            # busy bound; stop the query progress handler for the final commit.
            db.set_progress_handler(None, 0)
            assert_owned(self.layout)
            db.commit()
        finally:
            if db is not None:
                db.close()
            os.close(fd)

    def _open(self, path):
        assert_owned(self.layout)
        db = bind_connection(sqlite3.connect(path, timeout=0.05, factory=OwnedConnection), self.layout)
        db.execute("PRAGMA busy_timeout=50")
        db.execute("PRAGMA max_page_count=16384")
        return db

    def _schema(self, db):
        assert_owned(self.layout)
        db.execute("CREATE TABLE IF NOT EXISTS sources (key TEXT PRIMARY KEY, witness TEXT, payload TEXT, error TEXT)")
        columns = [row[1] for row in db.execute("PRAGMA table_info(sources)")]
        if columns != ["key", "witness", "payload", "error"]:
            raise sqlite3.DatabaseError("Unknown statistics derivative schema")
        db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
        db.commit()

    def _read(self, row):
        check_deadline(self.deadline)
        if self.budget.metadata_decodes >= DECODE_LIMIT:
            raise IndexLimit("decode_pending", "More statistics sources require bounded classification")
        identity = row["witness"]
        if identity is None:
            return None, diagnostic("source_unavailable", row["key"])
        size = identity[-1]
        if size > METADATA_BYTES:
            return None, diagnostic("source_oversized", row["key"])
        if self.budget.metadata_bytes + size > self.budget.metadata_limit:
            raise IndexLimit("decode_pending", "More statistics sources require bounded byte classification")
        self.budget.metadata_decodes += 1
        self.budget.metadata_bytes += size
        document = None
        try:
            # Nonblocking open is essential: a candidate can be replaced by a
            # FIFO after discovery. Qualify the opened descriptor before reading.
            fd = os.open(self.path(row), os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
            try:
                opened = os.fstat(fd)
                if not stat.S_ISREG(opened.st_mode):
                    return None, diagnostic("source_unavailable", row["key"])
                if witness(opened) != identity:
                    return None, diagnostic("source_changed", row["key"])
                raw = self._read_descriptor(fd, size)
                if len(raw) != size or witness(os.fstat(fd)) != identity:
                    return None, diagnostic("source_changed", row["key"])
            finally:
                os.close(fd)
            document = json.loads(raw)
            value = self._qualify(row, document)
            if witness(self.path(row).lstat()) != identity:
                return None, diagnostic("source_changed", row["key"])
            payload = json.dumps(value, allow_nan=False, separators=(",", ":"))
            if len(payload.encode()) > SOURCE_BYTES:
                return None, diagnostic("projection_oversized", row["key"])
            check_deadline(self.deadline)
            return value, None
        except IndexLimit:
            raise
        except Exception:
            error = diagnostic("source_unavailable", row["key"])
            try:
                if document is not None and witness(self.path(row).lstat()) == identity:
                    context = self._scope_context(row, document)
                    if context is not None:
                        error["context"] = context
            except Exception:
                pass
            return None, error

    def _scope_context(self, row, document):
        """Independently qualify current task scope when unrelated evidence is malformed."""
        if row["kind"] != "task" or not isinstance(document, dict):
            return None
        from .store import TaskRecord
        from .task_documents import decode
        keys = {"product", "product_version", "format", "v", "task_id", "backend", "session_id",
                "repo_path", "started_at", "finished_at", "status", "parent_task_id", "spawned_by",
                "continuation_of_task_id", "chain_root_task_id", "root_task_id"}
        record = asdict(decode({key: value for key, value in document.items() if key in keys}, row["id"], TaskRecord))
        # Missing defaults cannot establish a current nonterminal classification.
        if document.get("status") not in {"running", "completed", "failed", "cancelled", "timed_out"} or "started_at" not in document:
            return None
        return {key: record[key] for key in keys if key in record}

    def _read_descriptor(self, fd, size):
        """Read a qualified regular descriptor in deadline-checked bounded chunks."""
        chunks, remaining = [], size + 1
        while remaining:
            check_deadline(self.deadline)
            chunk = os.read(fd, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        check_deadline(self.deadline)
        return b"".join(chunks)

    def _qualify(self, row, document):
        if row["kind"] == "task":
            from .store import TaskRecord
            from .task_documents import decode
            record = asdict(decode(document, row["id"], TaskRecord))
            if record["status"] not in {"running", "completed", "failed", "cancelled", "timed_out"}:
                raise ValueError("Unknown task status")
            keep = ("task_id", "backend", "model", "repo_path", "started_at", "finished_at", "status", "title",
                    "parent_task_id", "continuation_of_task_id", "chain_root_task_id", "spawned_by", "root_task_id",
                    "accounting", "exit_code", "workflow_builder")
            return {key: record[key] for key in keep}
        if row["kind"] == "owner":
            from .workflow_receipt_documents import decode
            return decode(document, row["id"])
        from .workflow_run_documents import decode
        run = decode(document, row["id"])
        keep = ("workflow_run_id", "name", "workflow_id", "revision", "repo_path", "created_at", "status",
                "finished_at", "finished_at_sequence", "sequence", "parent_link", "orchestrator_session_owner_run_id")
        result = {key: run[key] for key in keep if key in run}
        activations = []
        for activation in run.get("activations", []):
            check_deadline(self.deadline)
            projected = {key: activation[key] for key in ("id", "node_id", "role", "status", "title") if key in activation}
            projected["tasks"] = [{key: attempt[key] for key in (
                "task_id", "status", "started_at", "finished_at", "execution_kind", "owner_task_id", "transport_task_id",
                "native_owner_execution_id", "candidate", "resume_task_id", "native_child_id") if key in attempt}
                for attempt in activation.get("tasks", [])]
            activations.append(projected)
        result["activations"] = activations
        return result

    def prepare(self):
        """Advance one resumable classification batch, committing completed work on exhaustion."""
        assert_owned(self.layout)
        try:
            roster = self.discover()
            with self.connect() as db:
                try:
                    old = {key: (json.loads(w), json.loads(p) if p else None, json.loads(e) if e else None)
                           for key, w, p, e in db.execute("SELECT key,witness,payload,error FROM sources")}
                except (ValueError, TypeError):
                    db.execute("DELETE FROM sources")
                    old = {}
                storage_bytes = db.execute("SELECT COALESCE(SUM(length(payload)),0) FROM sources").fetchone()[0]
                live = {r["key"] for r in roster}
                for key in old.keys() - live:
                    db.execute("DELETE FROM sources WHERE key=?", (key,))
                # Each mutation below is a bounded single-row operation. An SQLite
                # interrupt can roll back the whole transaction, losing progress;
                # use explicit checks between these writes instead.
                db.set_progress_handler(None, 0)
                pending, sources, progressed, exhausted_deadline = False, [], 0, False
                for position, row in enumerate(roster):
                    try:
                        check_deadline(self.deadline)
                        previous = old.get(row["key"])
                        if previous and previous[0] == row["witness"] and not (previous[2] and previous[2]["code"] == "source_changed"):
                            row.update(payload=previous[1], error=previous[2])
                        else:
                            payload, error = self._read(row)
                            if error and error["code"] == "source_changed":
                                # Retry a replacement once, with a new descriptor and witness.
                                row["witness"] = witness(self.path(row).lstat())
                                payload, error = self._read(row)
                                if error and error["code"] == "source_changed":
                                    raise IndexLimit("decode_pending", "Source replacement remains unsettled")
                            serialized = json.dumps(payload) if payload else None
                            storage_bytes += len(serialized or "") - len(json.dumps(previous[1]) if previous and previous[1] else "")
                            if storage_bytes > INDEX_BYTES // 2:
                                raise IndexLimit("history_limit", "Statistics projection storage limit exceeded")
                            db.execute("INSERT OR REPLACE INTO sources VALUES (?,?,?,?)",
                                       (row["key"], json.dumps(row["witness"]), serialized, json.dumps(error) if error else None))
                            row.update(payload=payload, error=error)
                            progressed += 1
                        sources.append(row)
                    except IndexLimit as exc:
                        if exc.code not in {"decode_pending", "discovery_budget"}:
                            raise
                        pending = True
                        row.update(payload=None, error=diagnostic("classification_pending", row["key"]))
                        sources.append(row)
                        if exc.code == "discovery_budget":
                            exhausted_deadline = True
                            sources.extend({**remaining, "payload": None,
                                            "error": diagnostic("classification_pending", remaining["key"])}
                                           for remaining in roster[position + 1:])
                            break
            failures = [row["error"] for row in sources if row.get("error") and row["error"]["code"] != "classification_pending"]
            # Source failures are classified against the query in project(); only
            # current independently qualified context may prove them irrelevant.
            state = "preparing" if pending else "ready"
            if exhausted_deadline and not progressed:
                state = "unavailable"
                failures.append(diagnostic("discovery_budget", "No source classification fits the foreground deadline"))
            return {"state": state, "sources": sources, "diagnostics": failures[:16],
                    "history_complete": not pending and not failures}
        except (IndexLimit, OSError, sqlite3.Error) as exc:
            return {"state": "unavailable", "sources": [], "history_complete": False,
                    "diagnostics": [diagnostic(getattr(exc, "code", "index_unavailable"), str(exc))]}

    def validate_sources(self, sources):
        """Refresh the bounded roster; the caller compares query-relevant generation.

        A changed terminal document outside the cohort is not a snapshot change.
        Every new/changed source must still be classified before that is known.
        """
        current = self.discover()
        known = {r["key"]: r for r in sources}
        refreshed = []
        for row in current:
            check_deadline(self.deadline)
            previous = known.get(row["key"])
            if previous is not None and row["witness"] == previous["witness"]:
                refreshed.append(previous)
                continue
            updated, error = self._read(row)
            if error and not error.get("context") or not updated and not error:
                return False
            row.update(payload=updated, error=error)
            refreshed.append(row)
        # Publish the refreshed observation together; a failed classification
        # never leaves callers mixing an old and partially updated source list.
        sources[:] = refreshed
        return True
