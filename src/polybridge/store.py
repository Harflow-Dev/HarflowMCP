"""On-disk record of dispatched tasks.

The registry is in-process, but the process is not stable: an MCP client may run several bridge
servers or restart one, and a server that did not spawn a task would otherwise have no idea it
exists — leaving a live `claude` running that no tool can report on or stop.

So each task also gets a small sidecar JSON beside its raw stream log. Any server can read those
back, replay the log for the outcome, and check whether the process is still alive.
"""

from __future__ import annotations

import json
import hashlib
import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import control, events, identity
from .backends import Accumulator
from .backends import get as get_backend
from .backends import resume_command
from .stream import parse_line

log = logging.getLogger(__name__)

RECORD_SUFFIX = ".meta.json"

# Ids are uuid4, but they arrive from a caller and become file paths, so the charset is enforced.
TASK_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")

TERMINAL_RECORD_STATUSES = frozenset({"completed", "failed", "timed_out", "cancelled"})

# Prompts can be huge; enough is kept to recognise a task without bloating the record.
PROMPT_PREVIEW_CHARS = 2000

TAIL_LINES_RETURNED = 20
TAIL_LINE_CHARS = 2000


@dataclass(frozen=True)
class TaskRecord:
    """What is known about a task without holding its subprocess."""

    task_id: str
    backend: str
    session_id: str | None
    repo_path: str
    started_at: str
    freedom: str = "write_in_repo"
    markers: list[str] = field(default_factory=list)
    """Strings that must appear in the process command line for a pid to still be this task."""
    pid: int | None = None
    pgid: int | None = None
    model: str | None = None
    reasoning_effort: str | None = None
    max_turns: int | None = None
    network: bool | None = None
    """The network request this run was dispatched with: True/False explicit, None the freedom's
    historical default. The *outcome* lives in `enforcement.network_access` — this records what
    was asked, so a resume can inherit it exactly. Defaults to None so a record written before
    this field existed still loads and resumes at the historical default; `read` already filters
    unknown keys and defaults missing ones, so no migration is needed."""
    parent_task_id: str | None = None
    stderr_tail: list[str] = field(default_factory=list)
    workflow_builder: bool = False
    prompt: str = ""
    prompt_source: str | None = None
    prompt_truncated: bool = False
    prompt_error: str | None = None
    status: str = "running"
    exit_code: int | None = None
    finished_at: str | None = None
    enforcement: dict[str, Any] | None = None
    """What was actually enforced, captured at spawn (`Task.enforcement`) rather than rebuilt from
    today's backend code — see `_enforcement`. None means the record predates this field existing,
    not that nothing was enforced; defaults to None so a record written before it existed still
    loads."""
    bridge_notices: list[str] = field(default_factory=list)
    """Notices the bridge itself generated about the dispatch, kept apart from the backend's own
    `Accumulator.notices` — see `Task.bridge_notices` for why."""
    start_time: str | None = None
    """The task's own process start time, from `identity.capture` — captured shortly after spawn,
    so it is null ("pending") until that capture lands, and null forever for a record written
    before this field existed. Paired with `markers` for `identity.identity_check`."""
    owner: dict[str, Any] | None = None
    """The bridge server process that dispatched this task — `identity.own_identity()` at spawn.
    Distinct from `pid`/`pgid`, which identify the task's own subprocess, not the server that
    started it. None for a record written before this field existed."""
    base_commit: str | None = None
    """`git rev-parse HEAD` in the repo at spawn, before the agent ran — None if the repo had no
    commit yet, or the probe failed or timed out."""
    start_dirty: bool | None = None
    """Whether `git status --porcelain` was non-empty at spawn — None if the probe failed, timed
    out, or predates this field."""
    spawned_by: str | None = None
    """The task_id of the task whose own agent dispatched this one via polybridge — best-effort,
    from `lineage.detect_caller`. None for a root task (no caller detected), and for any record
    written before this field existed."""
    root_task_id: str | None = None
    """The top of this dispatch chain: this task's own id for a root task, else inherited from the
    caller's `root_task_id`. None for a record written before this field existed."""
    depth: int = 0
    """How many nested dispatches deep this task is; 0 for a root task, and for a record written
    before this field existed."""
    max_depth: int | None = None
    """The nesting budget this task (and anything it spawns) is checked against — see
    `lineage.max_depth_default`. None for a record written before this field existed."""
    group: str | None = None
    """An optional caller-chosen label, inherited by nested dispatches unless overridden — see
    `TaskRegistry.start`'s `group` parameter."""
    title: str | None = None
    """An optional short human-readable label for the Monitor — see `start_task`'s `title`. A
    resume carries the resumed task's title; a fresh start never inherits one. None for a record
    written before this field existed."""
    lineage_detected: str | None = None
    """Which `lineage.detect_caller` method found this task's caller (`"pb_task_id"` | `"session"`
    | `"ancestry"`), or None if no caller was detected."""
    input_after_result: int | None = None
    """How many results the run had reported when polybridge last wrote a message to its stdin
    (live input). Until a later result exists in the stream, the run owes a turn, so an earlier
    success is not its outcome — see `_resolve`. None: no message was ever written after spawn."""
    live_input: bool = False
    """Whether the run was spawned with a stdin pipe that takes further messages (`send_message`,
    `polybridge-ctl send`). From the run's own `Invocation`, never inferred from the backend. False
    for a record written before this field existed — those runs all had stdin DEVNULL."""


class InvalidTaskId(ValueError):
    """A task id that must not be turned into a filesystem path."""


def validate_task_id(task_id: str) -> str:
    """Reject anything that could escape the task directory.

    Ids we mint are uuid4, but this value reaches us from a caller, and it is used to build file
    paths — `../` in it would otherwise read records outside the directory.
    """
    if not TASK_ID_PATTERN.fullmatch(task_id or ""):
        raise InvalidTaskId(f"not a valid task id: {task_id!r}")
    return task_id


def record_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{validate_task_id(task_id)}{RECORD_SUFFIX}"


def log_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{validate_task_id(task_id)}.jsonl"


def write(log_dir: Path, record: TaskRecord) -> None:
    """Persist a record, atomically. Never raises — losing a record must not fail a dispatch.

    Refuses to move a task backwards: with several server processes writing, a stale "running"
    record must not overwrite an already-recorded outcome.
    """
    write_landed(log_dir, record)


def write_landed(log_dir: Path, record: TaskRecord) -> bool:
    """`write`, reporting whether the record actually landed on disk.

    Same never-raise contract; False for an invalid id, a refused backwards move, or an I/O
    failure. For a caller that must tell the user when a status it published did not stick.
    """
    previous_directory_mtime = log_dir.stat().st_mtime_ns if log_dir.exists() else None
    try:
        target = record_path(log_dir, record.task_id)
    except InvalidTaskId:
        log.warning("refusing to persist a record for an invalid task id")
        return False

    existing = read(log_dir, record.task_id, include_prompt=False)
    if (
        existing is not None
        and existing.status in TERMINAL_RECORD_STATUSES
        and record.status not in TERMINAL_RECORD_STATUSES
    ):
        log.debug(
            "keeping recorded %s for task %s over stale %s",
            existing.status,
            record.task_id,
            record.status,
        )
        return False

    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        payload = asdict(record)
        if len(record.prompt) > PROMPT_PREVIEW_CHARS:
            digest = hashlib.sha256(record.prompt.encode("utf-8")).hexdigest()
            source = log_dir / f"{record.task_id}.prompt.{digest}.txt"
            if not source.exists():
                prompt_handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=log_dir, prefix=f".{record.task_id}.prompt.", delete=False)
                try:
                    with prompt_handle:
                        os.fchmod(prompt_handle.fileno(), 0o600)
                        prompt_handle.write(record.prompt)
                        prompt_handle.flush()
                        os.fsync(prompt_handle.fileno())
                    os.replace(prompt_handle.name, source)
                except BaseException:
                    Path(prompt_handle.name).unlink(missing_ok=True)
                    raise
            payload.update(prompt=record.prompt[:PROMPT_PREVIEW_CHARS], prompt_source=source.name, prompt_truncated=True)
        handle = tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=log_dir, prefix=f".{record.task_id}.", delete=False
        )
        try:
            with handle:
                json.dump(payload, handle, indent=2)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(handle.name, target)
        except BaseException:
            Path(handle.name).unlink(missing_ok=True)
            raise
    except OSError:
        log.warning("could not persist record for task %s", record.task_id, exc_info=True)
        return False
    from .catalog import Catalog
    header, stamp, active = listing_header(record)
    Catalog(log_dir, RECORD_SUFFIX).record(header, stamp, active, record.task_id, previous_directory_mtime=previous_directory_mtime)
    return True


def read(log_dir: Path, task_id: str, *, include_prompt: bool = True, metadata_byte_limit: int | None = None, metadata_budget: Any = None) -> TaskRecord | None:
    path = record_path(log_dir, task_id)
    try:
        if metadata_byte_limit is None:
            raw = json.loads(path.read_text(encoding="utf-8"))
        else:
            from .bounded_io import read_json
            raw = read_json(path, metadata_byte_limit, budget=metadata_budget)
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        from .bounded_io import ReadLimit
        if isinstance(exc, ReadLimit):
            raise
        log.warning("ignoring unreadable task record %s", path, exc_info=True)
        return None
    if not isinstance(raw, dict):
        return None

    fields = {f for f in TaskRecord.__dataclass_fields__}
    try:
        # Unknown keys are dropped so a record written by a newer version still loads.
        record = TaskRecord(**{k: v for k, v in raw.items() if k in fields})
        if include_prompt and record.prompt_truncated:
            try:
                source = record.prompt_source
                match = re.fullmatch(re.escape(task_id) + r"\.prompt\.([0-9a-f]{64})\.txt", source) if isinstance(source, str) else None
                if match is None:
                    raise ValueError("Invalid full prompt source")
                source_path = log_dir / source
                if source_path.is_symlink():
                    raise ValueError("Symlinked full prompt source")
                prompt = source_path.read_bytes().decode("utf-8")
                if hashlib.sha256(prompt.encode("utf-8")).hexdigest() != match[1]:
                    raise ValueError("Full prompt source integrity check failed")
                record = replace(record, prompt=prompt, prompt_truncated=False, prompt_error=None)
            except (ValueError, OSError) as exc:
                reason = "Full task assignment is unavailable; prompt preview is not authoritative: " + str(exc)
                log.warning("task %s: %s", task_id, reason)
                record = replace(record, prompt="", prompt_error=reason)
        return record
    except (TypeError, ValueError, OSError):
        log.warning("ignoring malformed task record or unavailable full prompt %s", path)
        return None


def read_all(log_dir: Path) -> list[TaskRecord]:
    try:
        paths = sorted(log_dir.glob(f"*{RECORD_SUFFIX}"))
    except OSError:
        return []
    records = [read(log_dir, path.name[: -len(RECORD_SUFFIX)], include_prompt=False) for path in paths]
    return sorted((r for r in records if r is not None), key=lambda r: r.started_at)


def listing_header(record: TaskRecord) -> tuple[dict[str, Any], float, bool]:
    """Listing metadata only: never reconstruct an outcome by replaying a stream."""
    keys = ('task_id', 'backend', 'session_id', 'repo_path', 'status', 'freedom', 'started_at',
            'parent_task_id', 'spawned_by', 'root_task_id', 'depth', 'max_depth', 'group', 'title',
            'lineage_detected', 'live_input', 'workflow_builder', 'owner', 'finished_at')
    header = {key: getattr(record, key) for key in keys}
    for key, value in list(header.items()):
        if isinstance(value, str):
            header[key] = value[:1000]
    if isinstance(record.owner, dict):
        header['owner'] = {key: value for key, value in record.owner.items() if key in {'pid', 'start_time'}}
    try:
        duration = _duration_seconds(record, record.finished_at)
    except (TypeError, ValueError):
        duration = None
    header.update(recovered=True, duration_seconds=duration, notices=[str(n)[:500] for n in record.bridge_notices[:4]])
    unobserved = outcome_unobserved(record)
    header.update(persisted_status=record.status, observed_exit=not unobserved, needs_reconciliation=unobserved, status_reconciled=False, process_identity_state=None)
    if unobserved:
        header['status'] = 'running'
    caller_keys = ('task_id', 'backend', 'session_id', 'repo_path', 'started_at', 'freedom', 'markers', 'pid', 'pgid', 'start_time', 'status', 'exit_code', 'root_task_id', 'parent_task_id', 'spawned_by', 'depth', 'max_depth', 'network', 'owner', 'group')
    header['_caller'] = {key: getattr(record, key) for key in caller_keys}
    try:
        stamp = datetime.fromisoformat(record.started_at).timestamp()
    except (ValueError, TypeError, OverflowError):
        stamp = 0.0
    return header, stamp, unobserved


def bootstrap_catalog(log_dir: Path) -> bool:
    from .catalog import Catalog
    def load(identifier: str, *, _metadata_budget=None):
        record = read(log_dir, identifier, include_prompt=False, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=_metadata_budget)
        return listing_header(record) if record is not None else None
    load.bounded_metadata = True
    catalog = Catalog(log_dir, RECORD_SUFFIX)
    with catalog.connect() as db:
        return catalog.bootstrap(db, load)


def _project_indexed_status(header: dict[str, Any], record: TaskRecord) -> bool:
    unobserved = outcome_unobserved(record)
    state = identity.identity_check(identity.task_identity(record.pid, record.start_time, record.markers)) if unobserved and record.pid is not None else ('undecidable' if unobserved else 'dead')
    state = 'uncertain' if state not in {'alive', 'dead'} else state
    active = unobserved and state != 'dead'
    if active:
        status = 'running'
        needs = state == 'uncertain' or record.status in TERMINAL_RECORD_STATUSES and not header.get('status_reconciled')
    elif not unobserved or header.get('status_reconciled'):
        status, needs = header['status'], False
    else:
        status = record.status if record.status in {'cancelled', 'completed', 'timed_out'} else 'unknown'
        needs = status == 'unknown'
        if needs:
            header['note'] = 'process ended; outcome needs inspection'
    header.update(status=status, persisted_status=record.status, observed_exit=not unobserved, process_identity_state=state, needs_reconciliation=needs)
    return active


def list_page(log_dir: Path, *, limit: int = 100, cursor: str | None = None, active_only: bool = False, session_id: str | None = None, task_ids: list[str] | None = None) -> dict[str, Any]:
    from .catalog import Catalog
    def load(identifier: str, *, _metadata_budget=None):
        record = read(log_dir, identifier, include_prompt=False, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=_metadata_budget)
        return listing_header(record) if record is not None else None
    load.bounded_metadata = True
    catalog = Catalog(log_dir, RECORD_SUFFIX)
    with catalog.connect() as db:
        # Upgrade existing headers without decoding metadata or losing chronology.
        db.execute("UPDATE entries SET active=1,payload=json_set(payload,'$.needs_reconciliation',json('true'),'$.observed_exit',json('false'),'$.persisted_status',json_extract(payload,'$.status'),'$.status','running') WHERE id IN (SELECT id FROM callers WHERE terminal=0) AND json_type(payload,'$.needs_reconciliation') IS NULL")
    if task_ids is not None:
        identifiers = [validate_task_id(identifier) for identifier in task_ids]
        page = {'items': catalog.headers(identifiers, load), 'next_cursor': None, 'has_more': False, 'bootstrap_pending': False}
    else:
        page = catalog.page(load, limit=limit, cursor=cursor, active_only=active_only, session_id=session_id)
    from .workflows import WorkflowStore
    workflow_storage = WorkflowStore(log_dir.parent)
    workflow_catalog = Catalog(workflow_storage.runs, '.json')
    for item in page['items']:
        try:
            from .bounded_io import read_receipt
            receipt = read_receipt(log_dir.parent / 'workflow-owners' / f"{validate_task_id(item['task_id'])}.json")
            allowed = {'workflow_run_id', 'workflow_node_id', 'workflow_execution_id', 'workflow_role', 'workflow_name', 'workflow_status', 'workflow_session_owner_run_id', 'root_workflow_run_id', 'interaction_owner', 'execution_contract'}
            item.update({key: str(value)[:100] if isinstance(value, str) else value for key, value in receipt.items() if key in allowed and isinstance(value, (str, bool, int, type(None)))})
            if 'workflow_role' not in receipt:
                from .workflows import WorkflowStore
                header = workflow_storage.get_run_header(receipt['workflow_run_id'], _catalog=workflow_catalog)
                if header.get('needs_direct_lookup'):
                    page.update(ownership_incomplete=True, history_incomplete=True, counts_complete=False, total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
                item.update(workflow_name=header.get('name'), workflow_status=header.get('status'), root_workflow_run_id=header.get('root_workflow_run_id', header['workflow_run_id']))
                if item.get('session_id') == header.get('sessions', {}).get('orchestrator') and header.get('orchestrator_session_owner_run_id'):
                    item['workflow_session_owner_run_id'] = header['orchestrator_session_owner_run_id']
        except (OSError, ValueError, TypeError) as exc:
            if isinstance(exc, ValueError):
                page.update(ownership_incomplete=True, history_incomplete=True, counts_complete=False, total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
            pass
    from .catalog import bound_header
    with catalog.connect() as db:
        for item in page['items']:
            row = db.execute('SELECT payload FROM callers WHERE id=?', (item['task_id'],)).fetchone()
            if row is None:
                continue
            record = TaskRecord(**json.loads(row[0]))
            active = _project_indexed_status(item, record)
            db.execute('UPDATE entries SET active=?,payload=? WHERE id=?', (int(active), json.dumps(bound_header(item), ensure_ascii=True, separators=(',', ':')), record.task_id))
        # Independently rotate <=100 other active identities. Older unloaded work
        # must eventually reconcile without requiring users to load all history.
        excluded = [item['task_id'] for item in page['items']]
        exclusion = ' AND e.id NOT IN (' + ','.join('?' for _ in excluded) + ')' if excluded else ''
        row = db.execute("SELECT value FROM state WHERE key='identity_after'").fetchone()
        boundary = json.loads(row[0]) if row else None
        selection = 'SELECT e.id,e.stamp,e.payload,c.payload FROM entries e JOIN callers c ON c.id=e.id WHERE e.active=1 AND c.terminal=0' + exclusion
        suffix = ' ORDER BY e.stamp DESC,e.id DESC LIMIT 100'
        rotating = db.execute(selection + (' AND (e.stamp<? OR (e.stamp=? AND e.id<?))' if boundary else '') + suffix, (*excluded, *((boundary[0], boundary[0], boundary[1]) if boundary else ()))).fetchall()
        if not rotating and boundary is not None:
            rotating = db.execute(selection + suffix, excluded).fetchall()
        for identifier, stamp, payload, caller in rotating:
            header = json.loads(payload)
            active = _project_indexed_status(header, TaskRecord(**json.loads(caller)))
            db.execute('UPDATE entries SET active=?,payload=? WHERE id=?', (int(active), json.dumps(bound_header(header), ensure_ascii=True, separators=(',', ':')), identifier))
        if rotating:
            db.execute('INSERT OR REPLACE INTO state VALUES (?,?)', ('identity_after', json.dumps([rotating[-1][1], rotating[-1][0]])))
        if 'total_active_count' in page and not page['bootstrap_pending'] and not page.get('history_incomplete'):
            where, values = (' AND session=?', (session_id,)) if session_id is not None else ('', ())
            unresolved = db.execute("SELECT COUNT(*) FROM entries WHERE active=1 AND (json_extract(payload,'$.process_identity_state') IS NULL OR json_extract(payload,'$.process_identity_state')='uncertain')" + where, values).fetchone()[0]
            page['counts_complete'] = not bool(unresolved)
            page['total_active_count'] = None if unresolved else db.execute('SELECT COUNT(*) FROM entries WHERE active=1' + where, values).fetchone()[0]
    page['items'] = [bound_header(item) for item in page['items']]
    related, visited = [], {item['task_id'] for item in page['items']}
    queue = [item[key] for item in page['items'] for key in ('parent_task_id', 'spawned_by', 'root_task_id') if item.get(key) and item[key] not in visited]
    with catalog.connect() as db:
        while queue and len(related) < 32 and len(visited) < 132:
            identifier = validate_task_id(queue.pop(0))
            if identifier in visited:
                continue
            visited.add(identifier)
            row = db.execute('SELECT payload FROM entries WHERE id=?', (identifier,)).fetchone()
            if row is None or not record_path(log_dir, identifier).exists():
                continue
            if catalog._changed(db, identifier):
                value = catalog._load_bounded(identifier, load)
                if value is None:
                    catalog._remove(db, identifier)
                    continue
                header, stamp, active = value
                catalog._put(db, header, stamp, active, identifier)
                row = db.execute('SELECT payload FROM entries WHERE id=?', (identifier,)).fetchone()
            header = json.loads(row[0])
            try:
                from .bounded_io import read_receipt
                receipt = read_receipt(log_dir.parent / 'workflow-owners' / f'{identifier}.json')
                header.update({key: str(value)[:100] if isinstance(value, str) else value for key, value in receipt.items() if key.startswith('workflow_') and isinstance(value, (str, bool, int, type(None)))})
                header = bound_header(header)
            except (OSError, ValueError, TypeError) as exc:
                if isinstance(exc, ValueError):
                    page.update(ownership_incomplete=True, history_incomplete=True, counts_complete=False, total_active_count=None, total_active_root_count=None, total_attention_root_count=None)
                pass
            related.append(header)
            queue[0:0] = [header[key] for key in ('parent_task_id', 'spawned_by', 'root_task_id') if header.get(key) and header[key] not in visited]
    page['related_headers'] = related
    return page


def process_alive(pid: int | None, markers: Sequence[str]) -> bool:
    """Whether the recorded process is still running *and* is still the task we think it is.

    The pid alone is not enough — pids get reused. Each backend supplies markers that must all
    appear in the command line for it to be the same run (see `tasks._identity_markers`).
    """
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # Alive but owned by someone else, so it is not ours.
        return False

    try:
        cmdline = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        # The pid is alive; we just cannot confirm identity. Reporting "gone" would be a worse
        # error than reporting "running", since it invents an outcome for a task still going.
        log.warning("could not run ps to identify pid %s; assuming it is still the task", pid)
        return True
    return all(marker in cmdline for marker in markers) if markers else True


def record_process_alive(record: TaskRecord) -> bool:
    """Whether the process a record describes is still running *and* is still that task.

    Uses the strongest evidence the record carries: `start_time`, captured at spawn. A backend
    may rename its own process after spawn — vibe 2.25.8 calls `setproctitle("Vibe CLI")`, so
    `ps -o command=` then shows `Vibe CLI` and its argv markers never match — so with a
    `start_time` the verdict comes from `identity.check_detail`, and anything but `dead` counts
    as alive: `alive`, and `undecidable` (incl. `markers_missing` and `ps_failed`), keeping the
    rule that a status must never manufacture a false "gone" for a run that is still going. A
    record with no `start_time` (legacy, or a capture that has not landed yet) falls back to
    today's `process_alive` pid+markers test. A missing or invalid pid is False: there is no
    process to be alive.

    Not for *signal authorisation*: `may_signal`/`signalable` stay stricter (a matching start time
    plus every marker, or the legacy `legacy_markers_seen` case) — see `identity`'s module
    docstring. The resume guards use it because for them uncertainty must refuse, which `True` does.
    """
    pid = record.pid
    if pid is None or pid <= 0:
        return False
    if record.start_time is not None:
        verdict, _ = identity.check_detail(
            identity.task_identity(pid, record.start_time, record.markers)
        )
        return verdict != "dead"
    return process_alive(pid, record.markers)


def replay_log(log_dir: Path, task_id: str, backend_name: str) -> tuple[Accumulator, list[str]]:
    """Rebuild what the stream said, so a recovered task reports the same fields as a live one."""
    backend = get_backend(backend_name)
    state = Accumulator()
    tail: list[str] = []
    try:
        with log_path(log_dir, task_id).open(encoding="utf-8") as handle:
            for line in handle:
                line = line.rstrip("\n")
                if not line.strip():
                    continue
                tail.append(line[:TAIL_LINE_CHARS])
                event = parse_line(line)
                if event is not None:
                    backend.ingest(event, state)
    except FileNotFoundError:
        pass
    except OSError:
        log.warning("could not replay stream log for task %s", task_id, exc_info=True)
    return state, tail[-TAIL_LINES_RETURNED:]


def _duration_seconds(record: TaskRecord, finished_at: str | None) -> float | None:
    try:
        start = datetime.fromisoformat(record.started_at)
    except ValueError:
        return None
    end = datetime.now(timezone.utc)
    if finished_at:
        try:
            end = datetime.fromisoformat(finished_at)
        except ValueError:
            pass
    return round((end - start).total_seconds(), 3)


def outcome_unobserved(record: TaskRecord) -> bool:
    """Whether the recorded status might be a lie about a process that is still running.

    A terminal status with no exit code was never observed to exit: it is what a bridge server
    writes about a live run while its own event loop is being torn down, and what it writes when
    its monitor crashes. The agent process survives both, so these records have to be rechecked
    against the process rather than believed.
    """
    return record.status == "running" or record.exit_code is None


_OWNED_BY_LIVE_SERVER_NOTE = (
    "Recovered from disk: this task is owned by a live polybridge server process (pid {pid}), "
    "which is reading its output and will record its outcome; this process sees only what that "
    "server has persisted so far. Status and cancellation work."
)


def resolve_status(
    log_dir: Path, record: TaskRecord, *, detail: bool = True
) -> tuple[str, str, Accumulator, list[str]]:
    """The single place a recovered task's status is decided.

    Shared by `snapshot` and `brief` so the same task can never be listed as one status and
    reported as another. `detail=False` (used by `brief`, `_poll_recovered`, and retention) skips
    replaying the stream log when the record is already terminal and was actually observed to
    finish — replay is by far the most expensive part of resolving a status, and a listing has no
    use for the summary/usage/etc. it would buy. `detail=True` (the default, used by `snapshot`)
    always replays.
    """
    status, note, state, tail, _owned = _resolve(log_dir, record, detail=detail)
    return status, note, state, tail


def _resolve(
    log_dir: Path, record: TaskRecord, *, detail: bool
) -> tuple[str, str, Accumulator, list[str], bool]:
    """Same as `resolve_status`, plus whether a live owning server was found — see
    `identity.identity_check`. Kept private so `resolve_status` can keep its existing 4-tuple
    shape for every caller and test that predates ownership."""
    unobserved = outcome_unobserved(record)

    if not detail and record.status in TERMINAL_RECORD_STATUSES and not unobserved:
        return (
            record.status,
            "Recovered from disk: recorded by the bridge server that ran it.",
            Accumulator(),
            [],
            False,
        )

    state, tail = replay_log(log_dir, record.task_id, record.backend)
    if record.input_after_result is not None and state.result_count <= record.input_after_result:
        # A message was written to the run after its last result in this stream, so a turn is
        # still owed: the replay alone cannot see that (the bridge's writes are not in the agent's
        # stdout), and an earlier success must not be read as the outcome.
        state.turn_open = True
    alive = unobserved and record_process_alive(record)

    if alive and record.status in TERMINAL_RECORD_STATUSES:
        owned = identity.identity_check(record.owner) == "alive"
        return (
            "running",
            f"Recovered from disk: this task is recorded as '{record.status}', but its process is "
            "still alive and working — that record was written by a bridge server being shut down "
            "mid-run, and it was wrong. Cancellation works. Its output, however, is going nowhere: "
            "the pipes died with that server, so the raw log stops at the teardown and no summary "
            "will ever arrive for the rest of the run. Judge it by what it changed on disk, or "
            "cancel it and dispatch again.",
            state,
            tail,
            owned,
        )
    if alive:
        owned = identity.identity_check(record.owner) == "alive"
        note = (
            _OWNED_BY_LIVE_SERVER_NOTE.format(pid=(record.owner or {}).get("pid"))
            if owned
            else "Recovered from disk: this task was started by an earlier bridge server process and "
            "is still running. Status and cancellation work; its live output is not being read by "
            "this process, so the summary appears only once it finishes."
        )
        return ("running", note, state, tail, owned)
    if record.status in TERMINAL_RECORD_STATUSES and not unobserved:
        return (
            record.status,
            "Recovered from disk: recorded by the bridge server that ran it.",
            state,
            tail,
            False,
        )
    if unobserved and not alive:
        # The process is gone and was never observed to exit, so every other branch below would
        # have to guess at why. If a cross-process cancel attempt landed on the still-alive leader
        # before it died, that is not a guess — it is exactly what happened, so report it rather
        # than reconstructing a status from a stream that has no result event for this either way.
        try:
            authorized = control.cancel_verdict(log_dir, record.task_id) == "authorized"
        except Exception:
            log.warning(
                "could not read the cross-process cancel verdict for task %s", record.task_id,
                exc_info=True,
            )
            authorized = False
        if authorized:
            return (
                "cancelled",
                "Recovered from disk: the process is gone and was never observed to exit, but a "
                "cross-process cancel delivered its SIGTERM to the still-alive leader before it "
                "died — reported as cancelled rather than reconstructed from its output.",
                state,
                tail,
                False,
            )
    # Only an unobserved `failed` is an inference the stream may overrule: it is what the monitor's
    # backstop writes when it never saw the process exit at all. Every other status records
    # something the bridge did — `cancelled` above all — so it stands even without an exit code.
    if record.status in TERMINAL_RECORD_STATUSES and record.status != "failed":
        return (
            record.status,
            "Recovered from disk: recorded by the bridge server that ran it, which never saw the "
            "process exit. The status stands because it describes what the bridge did, not what it "
            "guessed the run was doing.",
            state,
            tail,
            False,
        )
    if state.terminal is not None or state.saw_final_message:
        # `record.exit_code` is passed through as-is, None included. Substituting 0 here used to
        # make every backend look as though it had exited cleanly, so a recovered run carrying a
        # closing message was published as `completed` on no evidence at all. What that None is
        # worth is each backend's own business: claude's `result` event, opencode's
        # `reason: "stop"` and antigravity's `result` event are real terminal evidence, while
        # codex and vibe have no terminal event and so require an observed zero exit.
        status = get_backend(record.backend).classify(state, record.exit_code)
        return (
            status,
            "Recovered from disk: the bridge server that started this task went away before "
            "recording the outcome, so this was reconstructed from the run's own output. "
            + _reconstruction_note(status, state),
            state,
            tail,
            False,
        )
    return (
        "failed",
        "Recovered from disk: the process is gone and its output has no result event, so it was "
        "interrupted — most likely killed when the bridge server that started it exited. This is "
        "inferred rather than observed. Nothing it had already written to the repo was undone.",
        state,
        tail,
        False,
    )


def _reconstruction_note(status: str, state: Accumulator) -> str:
    """Why a recovered run reads the way it does, in terms of the evidence actually present.

    Chosen from the accumulator and the resulting status, never from the backend's name — the point
    of the seam is that nothing out here knows which backend produced the stream. Before `classify`
    took `int | None` this could be one fixed sentence, because a fabricated zero exit made every
    reconstruction look like a self-reported success; now a `failed` here often means "the output
    was not enough on its own", which is a different thing to report than a run that failed.
    """
    if status == "completed":
        return (
            "Its own terminal output reported completion, which this backend treats as evidence in "
            "its own right; the exit code itself was never observed."
        )
    if status == "timed_out":
        return "Its own terminal output reported that the run exhausted its turn cap."
    if state.is_error:
        return "The run reported an error in its own output."
    # Only reached with evidence present — the caller checks that first — so this is the
    # `failed`-despite-a-closing-message case, which is a different thing to report than a run that
    # failed on its own account.
    return (
        "A closing message was recovered, but this backend cannot call a run completed without an "
        "observed zero exit code — the agent having spoken is not proof the run finished — so this "
        "is an inference from missing evidence rather than an observed failure."
    )


def _enforcement(record: TaskRecord) -> dict[str, Any]:
    """What was actually enforced for this run — from what was captured at spawn, never re-derived.

    Re-deriving via `get_backend(record.backend).enforcement(record.freedom)` would report what
    *today's* backend code claims, not what this run's own version actually enforced: if a
    backend's freedom mapping is ever changed, every historical record would silently acquire the
    new claim. A record with nothing persisted predates that field, so it says plainly that
    nothing was recorded rather than inventing a positive claim it cannot back up.
    """
    try:
        if record.enforcement is not None:
            return dict(record.enforcement)
        return {
            "freedom": record.freedom,
            "recorded": False,
            "note": (
                "this run predates enforcement being persisted, so what actually applied to it "
                "cannot be restated"
            ),
        }
    except Exception:
        # Malformed data in an old or hand-edited record must not break recovery.
        log.debug("could not resolve enforcement for task %s", record.task_id)
        return {}


def _notices(record: TaskRecord, state: Accumulator) -> list[str]:
    """Bridge notices merged with the backend's own, bridge first since they describe the dispatch
    rather than the run — mirrors `Task._notices`. Neither source list is mutated."""
    return [*record.bridge_notices, *([record.prompt_error] if record.prompt_error else []), *state.notices]


def snapshot(log_dir: Path, record: TaskRecord) -> dict[str, Any]:
    """A recovered task's state, shaped like a live snapshot so callers need no special casing."""
    if record.prompt_truncated:
        authoritative = read(log_dir, record.task_id)
        if authoritative is None:
            raise ValueError("Full task assignment is unavailable; prompt preview is not authoritative")
        record = authoritative
    status, note, state, tail, owned = _resolve(log_dir, record, detail=True)
    from .catalog import Catalog
    header, stamp, _active = listing_header(record)
    header.update(status=status, needs_reconciliation=False, status_reconciled=True)
    Catalog(log_dir, RECORD_SUFFIX).record(header, stamp, status == 'running', record.task_id)
    from . import inbox
    pending = inbox.pending_messages(log_dir, record.task_id)
    if record.workflow_builder:
        from .workflows import builder_pending_messages
        try:
            pending = builder_pending_messages(log_dir, record.task_id, pending)
        except (OSError, ValueError, KeyError, TypeError):
            log.warning("task %s: workflow pending-message bookkeeping unavailable", record.task_id, exc_info=True)

    return {
        "pending_messages": pending,
        **({"prompt_source": record.prompt_source, "prompt_truncated": record.prompt_truncated, **({"prompt_error": record.prompt_error} if record.prompt_error else {})} if record.prompt_source or record.prompt_error else {}),
        "task_id": record.task_id,
        "workflow_builder": record.workflow_builder,
        "backend": record.backend,
        "session_id": record.session_id,
        "repo_path": record.repo_path,
        "status": status,
        "freedom": record.freedom,
        "started_at": record.started_at,
        "duration_seconds": _duration_seconds(record, record.finished_at),
        "parent_task_id": record.parent_task_id,
        "spawned_by": record.spawned_by,
        "root_task_id": record.root_task_id,
        "depth": record.depth,
        "max_depth": record.max_depth,
        "group": record.group,
        "title": record.title,
        "lineage_detected": record.lineage_detected,
        "live_input": record.live_input,
        "summary": state.summary,
        "is_error": state.is_error,
        "total_cost_usd": state.total_cost_usd,
        "num_turns": state.num_turns,
        "exit_code": record.exit_code,
        **({"stderr_tail": list(record.stderr_tail)} if status == "failed" and record.stderr_tail else {}),
        "permission_denials": state.denials,
        "last_output_tail": tail,
        "raw_stream_log": str(log_path(log_dir, record.task_id)),
        "model": record.model,
        "reasoning_effort": record.reasoning_effort,
        "max_turns": record.max_turns,
        "mcp_servers": state.mcp_servers,
        "available_tool_count": state.available_tool_count,
        "usage": state.usage,
        "notices": _notices(record, state),
        # The enforcement actually captured at spawn, not re-derived from today's code — see
        # `_enforcement`.
        "enforcement": _enforcement(record),
        "owner": record.owner,
        "owned_by_live_server": owned if status == "running" else None,
        "base_commit": record.base_commit,
        "start_dirty": record.start_dirty,
        "events_log": str(events.events_path(log_dir, record.task_id)),
        "resume_command": resume_command(record.backend, record.session_id, record.repo_path),
        "recovered": True,
        "note": note,
    } | control.taken_over_fields(log_dir, record.task_id)


def brief(log_dir: Path, record: TaskRecord) -> dict[str, Any]:
    """Listing shape for a recovered task, using the same status resolution as `snapshot`.

    Carries the bridge's own notices but not the backend's, matching `Task.brief` so `list_tasks`
    never mixes shapes between live and recovered entries. The backend's notices would need the
    stream replayed, which a listing should not pay for — and they are stream detail, which is what
    `snapshot` is for. Uses `detail=False`, so a settled, observed task costs no replay here either.
    """
    status, _, _, _, owned = _resolve(log_dir, record, detail=False)
    return {
        "task_id": record.task_id,
        "workflow_builder": record.workflow_builder,
        "backend": record.backend,
        "session_id": record.session_id,
        "repo_path": record.repo_path,
        "status": status,
        "freedom": record.freedom,
        "started_at": record.started_at,
        "duration_seconds": _duration_seconds(record, record.finished_at),
        "parent_task_id": record.parent_task_id,
        "spawned_by": record.spawned_by,
        "root_task_id": record.root_task_id,
        "depth": record.depth,
        "max_depth": record.max_depth,
        "group": record.group,
        "title": record.title,
        "lineage_detected": record.lineage_detected,
        "live_input": record.live_input,
        "notices": list(record.bridge_notices),
        "owner": record.owner,
        "owned_by_live_server": owned if status == "running" else None,
        "recovered": True,
    } | control.taken_over_fields(log_dir, record.task_id)


def _record_holds_session(record: TaskRecord) -> bool:
    """A record whose run may still be writing to its session: never observed to finish, and its
    process not confirmed `dead` (`undecidable` counts as live — never guess a session is free)."""
    return bool(
        record.session_id
        and outcome_unobserved(record)
        and record.pid is not None
        and identity.identity_check(
            identity.task_identity(record.pid, record.start_time, record.markers)
        )
        != "dead"
    )


def live_session_task_ids(log_dir: Path, session_id: str) -> set[str]:
    """Ids of the records whose run may still hold `session_id` — see `_record_holds_session`.
    Records only: takeover reservations are `control.takeover_reservations`."""
    return {
        record.task_id
        for record in read_all(log_dir)
        if record.session_id == session_id and _record_holds_session(record)
    }


def live_session_ids(log_dir: Path) -> set[str]:
    """Sessions with a still-running task according to disk, whichever server started it, plus
    sessions a takeover currently holds (`control.takeover_reservations`: from a takeover's `.req`
    until its window lapses or its attached terminal is confirmed gone).

    Uses the same "was this outcome actually observed?" test as `resolve_status`, so a run whose
    server was torn down mid-flight still counts as live here. Trusting its recorded status instead
    would let a second resume start against a session that is still being written to.

    Liveness itself is `identity.identity_check`, not `process_alive`: this is a control decision
    (whether a resume is safe to start), and `process_alive`'s bias toward "still going" is right
    for a status report but wrong here — see `identity`'s module docstring. A pid that cannot be
    ruled out (`undecidable`, e.g. `ps` unavailable) still counts as busy, on the same "never guess
    a session is free" rule; only a confirmed-`dead` pid clears it.
    """
    # A backend that mints its own id may have died before disclosing one; nothing to exclude.
    live = {record.session_id for record in read_all(log_dir) if _record_holds_session(record)}
    live.update(
        session_id
        for session_id in control.takeover_reservations(log_dir).values()
        if session_id
    )
    return live
