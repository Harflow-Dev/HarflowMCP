"""Best-effort detection of the caller that dispatched this task, for lineage recording.

A polybridge server can itself be dispatched *by* an agent running under an earlier polybridge
task — an MCP client nested inside a `claude`/`codex`/`opencode`/`vibe`/`agy` run this server
started.
Recording that relationship (`spawned_by`, `root_task_id`, `depth`, …) lets cancellation cascade
and the caps in `backends.base` compare a nested dispatch against its parent. None of this is
authoritative: it is inference from process state, and every method below can miss.

Three methods, tried in order, each weaker than the last:

1. **`PB_TASK_ID` in the environment.** Set on the child polybridge server's own env at spawn
   (`{**os.environ, PB_TASK_ID: ..., ...}`), so the *fastest* signal, when it survives. It does
   not always survive: codex and vibe filter the environment before handing it to their own
   nested MCP servers, so a dispatch made from inside a codex or vibe session that does that
   filtering never sees it at all — the env candidate only ever comes from a backend (or MCP
   client) that passes the environment through unfiltered. Because the value arrives from a
   process this code does not control, it is never trusted on its own: the named record must also
   be confirmed alive AND related to this process by session or ancestry (see below), so a stale
   or spoofed `PB_TASK_ID` naming an unrelated-but-alive record cannot be mistaken for the truth.
2. **Session match.** A nested MCP server runs inside the task's own process group —
   `os.getsid(0) == record.pgid` — because that task was spawned with `start_new_session=True`,
   which makes its pgid its own session id too. This finds the caller for claude, opencode, and
   any codex/vibe nested server that *did* inherit the environment (env filtering does not change
   the process group). Several alive records can share
   a session (rare, but possible after a resume); the most recently started one wins.
3. **Ancestry walk.** The weakest and slowest: walk `os.getpid()`'s parent chain via a system-wide
   `ps -axo pid=,ppid=` snapshot, looking for the first ancestor pid that is a recorded task. This
   is what vibe specifically needs — it gives each MCP tool call its own session/process, so a
   same-process-group check does not find the caller; the chain has to be climbed several levels
   to reach the polybridge server that spawned the vibe run in the first place.

Every candidate, from every method, is confirmed via `identity.identity_check` before it is
trusted: a `ps`-reused pid, a stale record, or a tampered environment must never be read as a real
caller. Detection failing outright (an unreadable `ps`, no candidates, nothing related) simply
returns None — a task with no detected caller is treated as a root task, not an error.
"""

from __future__ import annotations

import json

import logging
import os
import re
import subprocess
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from . import identity, store

log = logging.getLogger(__name__)

ENV_TASK_ID = "PB_TASK_ID"
ENV_ROOT_TASK_ID = "PB_ROOT_TASK_ID"
ENV_DEPTH = "PB_DEPTH"

DEFAULT_MAX_DEPTH = 2

PROCESS_TABLE_TTL_SECONDS = 5.0

# Same charset as `store.TASK_ID_PATTERN` — ids we mint are uuid4, but a value read off the
# environment is untrusted input, so it is validated before it is used to look anything up.
_TASK_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")


@dataclass(frozen=True)
class Caller:
    """A confirmed caller found by `detect_caller`, and which method found it."""

    record: store.TaskRecord
    method: str  # "pb_task_id" | "session" | "ancestry"


def max_depth_default(environ: Mapping[str, str] | None = None) -> int:
    """`PB_MAX_DEPTH` from the environment, or `DEFAULT_MAX_DEPTH` when unset, unparsable, or
    negative. 0 is a valid, deliberate choice (no nested dispatch permitted at all) and is left
    alone."""
    env = environ if environ is not None else os.environ
    raw = env.get("PB_MAX_DEPTH")
    if raw is None:
        return DEFAULT_MAX_DEPTH
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_MAX_DEPTH
    if value < 0:
        return DEFAULT_MAX_DEPTH
    return value


def _load_process_table() -> dict[int, int] | None:
    """One system-wide `pid -> ppid` scan. None on any failure — never raises."""
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,ppid="],
            env={**os.environ, "LC_ALL": "C"},
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    table: dict[int, int] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            pid, ppid = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        table[pid] = ppid
    return table


_table_lock = threading.Lock()
_table_cache: dict[int, int] | None = None
_table_cached_at: float | None = None


def process_table() -> dict[int, int] | None:
    """The system's pid -> ppid map, cached module-wide for `PROCESS_TABLE_TTL_SECONDS`.

    A full-machine `ps` scan is not cheap enough to repeat at every call site that wants one — the
    session and ancestry methods can both want it in the same `detect_caller` call — so one scan
    is shared under a lock and only refreshed once the TTL has elapsed. Thread-safe; never raises.
    """
    global _table_cache, _table_cached_at
    now = time.monotonic()
    with _table_lock:
        if (
            _table_cache is not None
            and _table_cached_at is not None
            and now - _table_cached_at < PROCESS_TABLE_TTL_SECONDS
        ):
            return _table_cache
        table = _load_process_table()
        _table_cache = table
        _table_cached_at = now
        return table


# A stable reference to the real loader, captured before any caller can shadow the module-level
# name `process_table` with a local parameter of the same name (as `detect_caller` does, per the
# API contract) — so the default in that function still reaches the real cache.
_default_process_table = process_table


def ancestors(pid: int, table: Mapping[int, int]) -> list[int]:
    """`pid`'s parent chain, excluding `pid` itself, stopping at 0/1 or a cycle without including
    it. `table` is a `pid -> ppid` map, typically from `process_table()`."""
    chain: list[int] = []
    seen = {pid}
    current = pid
    while True:
        parent = table.get(current)
        if parent is None or parent in seen or parent in (0, 1):
            break
        chain.append(parent)
        seen.add(parent)
        current = parent
    return chain


def lineage_closure(
    rows: Iterable[tuple[str, str | None, str | None]], task_id: str
) -> set[str]:
    """`task_id`, every task reachable from it by following `spawned_by` downwards, and every task
    whose `root_task_id` names it. `rows` are `(task_id, spawned_by, root_task_id)` for every known
    task — live or not, so a settled intermediate does not break the chain to a live descendant."""
    rows = list(rows)
    children: dict[str, set[str]] = {}
    for tid, spawned_by, _root in rows:
        if spawned_by:
            children.setdefault(spawned_by, set()).add(tid)

    targets = {task_id}
    frontier = [task_id]
    while frontier:
        current = frontier.pop()
        for child in children.get(current, ()):
            if child not in targets:
                targets.add(child)
                frontier.append(child)

    targets.update(tid for tid, _spawned_by, root in rows if root == task_id)
    return targets


def _candidate_identity(record: store.TaskRecord) -> dict[str, Any]:
    """The identity dict `identity.identity_check` expects for `record`."""
    result = identity.task_identity(record.pid, record.start_time, record.markers)
    from .backends import get
    titles = getattr(get(record.backend), "caller_process_titles", ())
    if titles:
        result["caller_process_titles"] = titles
    return result


def _confirmed(record: store.TaskRecord, check: Callable[[Mapping[str, Any]], str]) -> bool:
    return check(_candidate_identity(record)) == "alive"


def _candidates(log_dir: Path) -> list[store.TaskRecord]:
    """Every record that could still be a live caller: not observed-terminal.

    "Observed-terminal" mirrors `store.outcome_unobserved`'s inverse — a terminal status *with* an
    exit code is a run this process actually saw finish, so it can never be today's caller.
    """
    return [
        record
        for record in store.read_all(log_dir)
        if not (record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None)
    ]


def _own_sid(getsid: Callable[[int], int]) -> int | None:
    try:
        return getsid(0)
    except OSError:
        return None


def _matches_session(record: store.TaskRecord, getsid: Callable[[int], int]) -> bool:
    sid = _own_sid(getsid)
    return sid is not None and record.pgid is not None and record.pgid == sid


def _matches_ancestry(
    record: store.TaskRecord,
    getpid: Callable[[], int],
    table_fn: Callable[[], Mapping[int, int] | None],
) -> bool:
    if record.pid is None:
        return False
    table = table_fn()
    if table is None:
        return False
    return record.pid in ancestors(getpid(), table)


def _by_pb_task_id(
    log_dir: Path,
    env: Mapping[str, str],
    candidates: list[store.TaskRecord],
    getsid: Callable[[int], int],
    getpid: Callable[[], int],
    table_fn: Callable[[], Mapping[int, int] | None],
    check: Callable[[Mapping[str, Any]], str],
) -> Caller | None:
    raw = env.get(ENV_TASK_ID)
    if not raw or not _TASK_ID_RE.fullmatch(raw):
        return None
    record = next((r for r in candidates if r.task_id == raw), None)
    if record is None or not _confirmed(record, check):
        return None
    # A confirmed-alive record is not enough on its own: `PB_TASK_ID` is untrusted input, and a
    # stale or spoofed value could coincidentally name a live-but-unrelated record. Only trust it
    # once it is also related to this process by session or ancestry.
    if _matches_session(record, getsid) or _matches_ancestry(record, getpid, table_fn):
        return Caller(record, "pb_task_id")
    return None


def _by_session(
    candidates: list[store.TaskRecord],
    getsid: Callable[[int], int],
    check: Callable[[Mapping[str, Any]], str],
) -> Caller | None:
    sid = _own_sid(getsid)
    if sid is None:
        return None
    matches = [r for r in candidates if r.pgid == sid and _confirmed(r, check)]
    if not matches:
        return None
    latest = max(matches, key=lambda r: r.started_at)
    return Caller(latest, "session")


def _by_ancestry(
    candidates: list[store.TaskRecord],
    getpid: Callable[[], int],
    table_fn: Callable[[], Mapping[int, int] | None],
    check: Callable[[Mapping[str, Any]], str],
) -> Caller | None:
    table = table_fn()
    if table is None:
        return None
    chain = ancestors(getpid(), table)
    # Several records can share a pid once it has been reused, so every one is checked: keeping
    # only the first (oldest) would let a dead record shadow the live task now holding that pid.
    by_pid: dict[int, list[store.TaskRecord]] = {}
    for record in candidates:
        if record.pid is not None:
            by_pid.setdefault(record.pid, []).append(record)
    for pid in chain:
        for record in reversed(by_pid.get(pid, [])):
            if _confirmed(record, check):
                return Caller(record, "ancestry")
    return None


def detect_caller(
    log_dir: Path,
    *,
    environ: Mapping[str, str] | None = None,
    getsid: Callable[[int], int] = os.getsid,
    getpid: Callable[[], int] = os.getpid,
    process_table: Callable[[], Mapping[int, int] | None] | None = None,
    check: Callable[[Mapping[str, Any]], str] | None = None,
) -> Caller | None:
    """Find the task (if any) that dispatched the current process, best-effort.

    Tries `PB_TASK_ID` first, then a same-session match, then an ancestry walk — see the module
    docstring for why each exists and what it depends on. Every candidate must independently be
    confirmed `alive` via `check`; precedence among the three methods is fixed regardless of which
    would find a record first. Never raises: any failure (an unreadable `ps`, a corrupt record, a
    getsid error) is logged at debug and treated as "no caller found", per CLAUDE.md's rule that
    bookkeeping must never change an outcome.

    `check` defaults to `identity.identity_check`, resolved through the module attribute at call
    time rather than bound as the parameter's default value — a default bound at import time would
    freeze in the original function object, so `monkeypatch.setattr(identity, "identity_check",
    ...)` would silently not apply to a caller (like `TaskRegistry`) that never passes `check`
    explicitly.
    """
    table_fn = process_table if process_table is not None else _default_process_table
    env = environ if environ is not None else os.environ
    check = check if check is not None else identity.identity_check
    try:
        candidates = _candidates(log_dir)
        found = _by_pb_task_id(log_dir, env, candidates, getsid, getpid, table_fn, check)
        if found is not None:
            return found
        found = _by_session(candidates, getsid, check)
        if found is not None:
            return found
        return _by_ancestry(candidates, getpid, table_fn, check)
    except Exception:
        log.debug("caller detection failed", exc_info=True)
        return None


# The real function, bound before anything can replace the module attribute (tests stub
# `detect_caller` so registries never walk the real process tree): `detect_caller_detail` is a
# separate seam with its own stub, and must not silently inherit that one.
_real_detect_caller = detect_caller


@dataclass(frozen=True)
class Detection:
    """`detect_caller_detail`'s answer. `caller` is a confirmed caller, or None. When it is None,
    `undecidable` says why "no caller" could not be *established* — None there means the negative
    was actually checked: the process table was readable, this process was in it, and every
    record related to this process by session or ancestry was confirmed `dead`."""

    caller: Caller | None
    undecidable: str | None = None


def detect_caller_detail(
    log_dir: Path,
    *,
    environ: Mapping[str, str] | None = None,
    getsid: Callable[[int], int] = os.getsid,
    getpid: Callable[[], int] = os.getpid,
    process_table: Callable[[], Mapping[int, int] | None] | None = None,
    check: Callable[[Mapping[str, Any]], str] | None = None,
) -> Detection:
    """`detect_caller` for a gate that must fail closed (takeover is for people only).

    `detect_caller` returns None both when there is positively no caller and when it could not
    look — `ps` missing or denied, an unreadable session, an exception — which is the right answer
    for lineage (a task with no detected caller is simply a root) and the wrong one for a gate. This
    tells the two apart: `Detection(None, None)` only once the negative was actually established.
    """
    table_fn = process_table if process_table is not None else _default_process_table
    check_fn = check if check is not None else identity.identity_check
    try:
        caller = _real_detect_caller(
            log_dir,
            environ=environ,
            getsid=getsid,
            getpid=getpid,
            process_table=table_fn,
            check=check_fn,
        )
        if caller is not None:
            return Detection(caller)
        table = table_fn()
        if table is None:
            return Detection(
                None,
                "the process table could not be read (ps failed or was denied), so this "
                "process's ancestry cannot be checked",
            )
        own_pid = getpid()
        if own_pid not in table:
            return Detection(
                None, "this process is missing from the process table, so its ancestry is unknown"
            )
        sid = _own_sid(getsid)
        if sid is None:
            return Detection(None, "this process's session id could not be read")
        chain = set(ancestors(own_pid, table))
        for record in _candidates(log_dir):
            related = (record.pgid is not None and record.pgid == sid) or (
                record.pid is not None and record.pid in chain
            )
            if related and check_fn(_candidate_identity(record)) != "dead":
                # Not confirmed alive (detect_caller would have returned it), not confirmed gone.
                return Detection(
                    None,
                    f"task {record.task_id} is related to this process and could not be "
                    "confirmed alive or gone",
                )
        return Detection(None)
    except Exception as exc:
        log.debug("caller detection failed", exc_info=True)
        return Detection(None, f"caller detection failed: {type(exc).__name__}: {exc}")


def detect_catalog_caller(log_dir: Path, *, environ: Mapping[str, str] | None = None,
                          getsid: Callable[[int], int] = os.getsid, getpid: Callable[[], int] = os.getpid,
                          process_table: Callable[[], Mapping[int, int] | None] | None = None,
                          check: Callable[[Mapping[str, Any]], str] | None = None) -> Detection:
    """Fail-closed authority for bounded APIs, querying only related indexed identities.

    The caller must complete bounded catalog bootstrap first. Legacy detection is unchanged.
    No missing/incomplete catalog ever falls back to historical metadata scanning.
    """
    from .catalog import Catalog
    env = environ if environ is not None else os.environ
    check_fn = check if check is not None else identity.identity_check
    table_fn = process_table if process_table is not None else _default_process_table
    try:
        table = table_fn()
        own_pid, sid = getpid(), _own_sid(getsid)
        if table is None or own_pid not in table or sid is None:
            return Detection(None, 'Process ancestry/session cannot be checked')
        chain = ancestors(own_pid, table)[:100]
        index = Catalog(log_dir, store.RECORD_SUFFIX)
        with index.connect() as db:
            index._discover(db)
            if index.state(db)['status'] != 'ready':
                return Detection(None, 'Task catalog bootstrap is incomplete')
            placeholders = ','.join('?' for _ in chain) or 'NULL'
            rows = db.execute(f'SELECT payload FROM callers WHERE terminal=0 AND (pgid=? OR pid IN ({placeholders}) OR id=?) LIMIT 101', (sid, *chain, env.get(ENV_TASK_ID, ''))).fetchall()
        if len(rows) > 100:
            return Detection(None, 'More than 100 related caller identities require reconciliation')
        candidates = [store.TaskRecord(**json.loads(row[0])) for row in rows]
        found = _by_pb_task_id(log_dir, env, candidates, getsid, getpid, lambda: table, check_fn)
        found = found or _by_session(candidates, getsid, check_fn) or _by_ancestry(candidates, getpid, lambda: table, check_fn)
        if found is not None:
            return Detection(found)
        if env.get(ENV_TASK_ID):
            return Detection(None, 'Workflow caller task identity cannot be verified')
        for record in candidates:
            if check_fn(_candidate_identity(record)) != 'dead':
                return Detection(None, f'Task {record.task_id} caller identity is uncertain')
        return Detection(None)
    except Exception as exc:
        return Detection(None, f'Catalog caller detection failed: {type(exc).__name__}: {exc}')
