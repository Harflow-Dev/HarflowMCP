"""Cross-process control: per-attempt phase files, and the two locks built on the same primitive.

A cancellation (and, later, a takeover — A4) has to be coordinated across server processes that
share nothing but the filesystem. The design here is deliberately conservative: every step that
matters is a single atomic filesystem operation (a hard link, which only one of several racing
writers can win), and every read either finds a fact or admits it could not tell — never a guess.

**Phase files.** A cancel attempt is named `<task_id>.cancel.<n>.<phase>`, `phase` one of
`req | sig | failed`. `n` is a positive decimal with no leading zero; a controller "owns" an attempt
by being the one whose `write_phase(..., "req", ...)` call actually created the file — a concurrent
loser joins the same attempt instead of starting its own. `.sig` records that the attempt's SIGTERM
was actually delivered (and, at that moment, whether the leader process was still alive to receive
it); `.failed` records that it wasn't, whether because the attempt's own controller gave up or
because a later controller found the original controller dead and recovered it. Existence alone
decides the outcome (`attempt_outcome`) — a payload only matters for `leader_alive` and for lease
bookkeeping.

**Lease recovery.** A controller mid-cancel might crash before writing `.sig`/`.failed`, leaving the
attempt `pending` forever. `.req` carries a lease (`lease_seconds`) and the controller's own
identity; `controller_abandoned` is only True once *both* the lease has expired *and*
`identity.identity_check` says the controller is `dead` — an `undecidable` controller is never
recovered, on the same "never guess" rule as everywhere else in this module.

**Locks.** `acquire`/`release`/`session_lock` are `flock` on a lock file, non-blocking with an async
retry loop so a held lock never blocks the event loop. `write_record_if_open` is the synchronous
record-lock primitive, meant to run in a worker thread (`asyncio.to_thread`): it takes the lock with
a *blocking* retry (this runs off the loop, so blocking is fine), re-reads the record under the
lock, and only writes when the record is still open for it to act on. Lock files are never deleted —
a sweep that removed one out from under a waiter would turn its `flock` into a lock on a description
of a file nobody can see anymore.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import logging
import os
import re
import tempfile
import time
import uuid
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from . import identity, store

log = logging.getLogger(__name__)

CANCEL = "cancel"
LEASE_SECONDS = 60

RECORD_LOCK_TIMEOUT_SECONDS = 10.0
_LOCK_POLL_SECONDS = 0.05

# `n` in a phase filename: a positive decimal, no leading zero. Anything else is ignored by
# `latest_attempt` — retention keeps its own separate, conservative "unparsable n => active" rule.
_ATTEMPT_N_RE = re.compile(r"^[1-9][0-9]{0,8}$")
_PHASE_FILE_RE = re.compile(r"^(?P<id>[^.]+)\.(?P<family>[^.]+)\.(?P<n>[^.]+)\.(?P<phase>[^.]+)$")

_SAFE_SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$")


class PhaseWriteError(OSError):
    """A phase file could not be written, for a reason other than it already existing."""


class LockTimeout(TimeoutError):
    """A lock (record or session) could not be acquired before its deadline."""


class _Unparsable:
    """Sentinel distinguishing "no such phase file" (None) from "a phase file exists but its
    payload cannot be trusted" (this) — the latter must be treated as undecidable, never as
    evidence either way, exactly like `identity.IdentityCheck`'s own `undecidable`."""

    def __repr__(self) -> str:
        return "UNPARSABLE"


UNPARSABLE = _Unparsable()


@dataclass(frozen=True)
class Attempt:
    """The attempt `begin_attempt` settled on: `n` is its number, `owned` is whether *this* call
    was the one that created it (False means it joined an attempt already underway)."""

    n: int
    owned: bool


def phase_path(log_dir: Path, task_id: str, family: str, n: int, phase: str) -> Path:
    store.validate_task_id(task_id)
    return log_dir / f"{task_id}.{family}.{n}.{phase}"


def _fsync_dir(log_dir: Path) -> None:
    """Best-effort: some platforms refuse to fsync a directory at all, and that must not be
    mistaken for the link itself having failed — the link's own errors are never swallowed here."""
    try:
        fd = os.open(log_dir, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def write_phase(log_dir: Path, task_id: str, family: str, n: int, phase: str, payload: dict) -> bool:
    """Create `<task_id>.<family>.<n>.<phase>` atomically, exactly once.

    Returns True if this call created it, False if it already existed (another controller/attempt
    already did this step — the caller joins rather than treating that as an error). Any other
    failure raises `PhaseWriteError`; either way the temp file used to build it is removed.
    """
    store.validate_task_id(task_id)
    target = phase_path(log_dir, task_id, family, n, phase)
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(dir=log_dir, prefix=f".{task_id}.")
    except OSError as exc:
        raise PhaseWriteError(f"could not write phase file {target}: {exc}") from exc
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(tmp_path, target)
    except FileExistsError:
        return False
    except OSError as exc:
        raise PhaseWriteError(f"could not write phase file {target}: {exc}") from exc
    finally:
        tmp_path.unlink(missing_ok=True)
    _fsync_dir(log_dir)
    return True


def read_phase(log_dir: Path, task_id: str, family: str, n: int, phase: str) -> dict | None | Any:
    """The phase file's payload: None if it does not exist, `UNPARSABLE` if it exists but cannot be
    trusted (unreadable, not JSON, or not a JSON object), else the parsed dict."""
    path = phase_path(log_dir, task_id, family, n, phase)
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        return UNPARSABLE
    try:
        data = json.loads(raw)
    except ValueError:
        return UNPARSABLE
    if not isinstance(data, dict):
        return UNPARSABLE
    return data


def latest_attempt(log_dir: Path, task_id: str, family: str) -> int | None:
    """The highest validly-numbered attempt on disk for `task_id`/`family`, or None if there is
    none. An unparsable `n` is silently skipped here — see `_ATTEMPT_N_RE`."""
    store.validate_task_id(task_id)
    best: int | None = None
    try:
        entries = list(log_dir.iterdir())
    except OSError:
        return None
    for path in entries:
        match = _PHASE_FILE_RE.match(path.name)
        if match is None:
            continue
        if match.group("id") != task_id or match.group("family") != family:
            continue
        n_str = match.group("n")
        if not _ATTEMPT_N_RE.match(n_str):
            continue
        n = int(n_str)
        if best is None or n > best:
            best = n
    return best


def attempt_outcome(log_dir: Path, task_id: str, family: str, n: int) -> str:
    """`"sig"` if the attempt's signal was delivered, else `"failed"` if it gave up (its own
    controller, or a later recovery), else `"pending"`. Existence alone decides this — a payload is
    never needed to tell these three states apart."""
    if phase_path(log_dir, task_id, family, n, "sig").exists():
        return "sig"
    if phase_path(log_dir, task_id, family, n, "failed").exists():
        return "failed"
    return "pending"


JOIN_PREFIX = "join-"
NOSIG_PREFIX = "nosig-"
_JOIN_TOKEN_RE = re.compile(r"^[0-9a-f]{32}$")


class JoinsUnreadable(OSError):
    """The task directory could not be enumerated, so which joiner intents exist is unknown."""


def join_tokens(log_dir: Path, task_id: str, family: str, n: int) -> list[str]:
    """Tokens of every joiner intent (`<id>.<family>.<n>.join-<token>`) recorded for attempt `n`.

    Raises `JoinsUnreadable` when the directory cannot be listed: an empty answer there would read
    as "no joiners" and let a `.failed` settle an attempt early.
    """
    store.validate_task_id(task_id)
    try:
        entries = list(log_dir.iterdir())
    except OSError as exc:
        raise JoinsUnreadable(f"could not list {log_dir}: {exc}") from exc
    tokens: list[str] = []
    for path in entries:
        match = _PHASE_FILE_RE.match(path.name)
        if match is None or match.group("id") != task_id or match.group("family") != family:
            continue
        if match.group("n") != str(n) or not match.group("phase").startswith(JOIN_PREFIX):
            continue
        token = match.group("phase")[len(JOIN_PREFIX) :]
        if _JOIN_TOKEN_RE.match(token):
            tokens.append(token)
    return tokens


def joins_outstanding(
    log_dir: Path, task_id: str, family: str, n: int, now: datetime | None = None
) -> bool:
    """Whether a joining controller of attempt `n` may still have a delivery to record.

    A joiner writes its intent before signalling, then resolves it with the shared `.sig` (it
    delivered) or its own `nosig-<token>` (it did not). An intent whose joiner is dead with an
    expired lease is resolved too — it can no longer record anything. An unparsable intent is
    undecidable, so it stays outstanding, like an unparsable `.req`.
    """
    if phase_path(log_dir, task_id, family, n, "sig").exists():
        return False
    resolved_now = now or datetime.now(timezone.utc)
    try:
        tokens = join_tokens(log_dir, task_id, family, n)
    except JoinsUnreadable:
        # Undecidable, like an unreadable intent file: treated as outstanding.
        return True
    for token in tokens:
        if phase_path(log_dir, task_id, family, n, f"{NOSIG_PREFIX}{token}").exists():
            continue
        intent = read_phase(log_dir, task_id, family, n, f"{JOIN_PREFIX}{token}")
        if controller_abandoned(intent, resolved_now):
            continue
        return True
    return False


def attempt_state(
    log_dir: Path, task_id: str, family: str, n: int, now: datetime | None = None
) -> str:
    """`attempt_outcome`, except that a `.failed` only settles the attempt once no joiner delivery
    is outstanding: a joiner that signalled may still be writing its `.sig`, and a `.failed` read
    first would publish the ordinary classified result for a run that was in fact cancelled."""
    outcome = attempt_outcome(log_dir, task_id, family, n)
    if outcome == "failed" and joins_outstanding(log_dir, task_id, family, n, now):
        return "pending"
    return outcome


def begin_join(
    log_dir: Path, task_id: str, family: str, n: int, controller: dict, now: datetime | None = None
) -> str:
    """Record a joining controller's intent to deliver under attempt `n`, before it signals.

    Returns the token its outcome is recorded under (`mark_join_undelivered`, or the shared `.sig`).
    Carries a lease like `.req`, so a joiner that dies mid-delivery is recoverable. Raises
    `PhaseWriteError` — and the caller must then not signal at all.
    """
    resolved_now = now or datetime.now(timezone.utc)
    payload = {"at": resolved_now.isoformat(), "by": controller, "lease_seconds": LEASE_SECONDS}
    while True:
        token = uuid.uuid4().hex
        if write_phase(log_dir, task_id, family, n, f"{JOIN_PREFIX}{token}", payload):
            return token


def nosig_phase(token: str) -> str:
    return f"{NOSIG_PREFIX}{token}"


def controller_abandoned(req_payload: Any, now: datetime) -> bool:
    """Whether the controller that opened a `.req` can be declared gone.

    True only when the payload is a well-formed `{"at", "by", "lease_seconds"}`, its lease has
    expired as of `now`, *and* `identity.identity_check(by)` says `dead` outright — `undecidable`
    (e.g. `ps` unavailable) is never enough, so an attempt with no way to check its controller is
    never recovered rather than recovered on a guess.
    """
    if not isinstance(req_payload, dict):
        return False
    at_raw = req_payload.get("at")
    lease = req_payload.get("lease_seconds")
    if not isinstance(at_raw, str) or not isinstance(lease, (int, float)) or isinstance(lease, bool):
        return False
    try:
        at = datetime.fromisoformat(at_raw)
    except ValueError:
        return False
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    try:
        if now < at + timedelta(seconds=lease):
            return False
    except (OverflowError, TypeError):
        return False
    return identity.identity_check(req_payload.get("by")) == "dead"


def recover_abandoned(
    log_dir: Path, task_id: str, family: str, n: int, now: datetime | None = None
) -> bool:
    """If attempt `n` is pending and its controller is abandoned, close it with `.failed
    {"reason": "controller died"}`. Returns whether it was (just now, or already) recovered — an
    already-`.failed` attempt found here again returns False, since this call did no work."""
    resolved_now = now or datetime.now(timezone.utc)
    if attempt_outcome(log_dir, task_id, family, n) != "pending":
        return False
    req = read_phase(log_dir, task_id, family, n, "req")
    if not controller_abandoned(req, resolved_now):
        return False
    write_phase(
        log_dir,
        task_id,
        family,
        n,
        "failed",
        {"at": resolved_now.isoformat(), "reason": "controller died"},
    )
    return True


def begin_attempt(
    log_dir: Path, task_id: str, family: str, controller: dict, now: datetime | None = None
) -> Attempt:
    """Start a new attempt, or join the one already underway.

    A new attempt (`n = latest + 1`, or `n = 1` if there is none yet) is opened only when the latest
    attempt has failed — including one just recovered here because its controller was abandoned.
    Otherwise (pending with a live-or-undecidable controller, or already signalled) the caller joins
    the latest attempt instead: no `.req` is written, and `owned` is False. Losing the race to
    create a fresh attempt (another controller's `write_phase` won) also joins, on the same n.
    """
    resolved_now = now or datetime.now(timezone.utc)
    latest = latest_attempt(log_dir, task_id, family)
    if latest is not None:
        if attempt_outcome(log_dir, task_id, family, latest) == "pending":
            recover_abandoned(log_dir, task_id, family, latest, resolved_now)
        # A `.failed` with a joiner delivery still outstanding is not settled: starting n+1 then
        # would hide that joiner's `.sig` from everything that reads only the latest attempt.
        if attempt_state(log_dir, task_id, family, latest, resolved_now) != "failed":
            return Attempt(n=latest, owned=False)
        n = latest + 1
    else:
        n = 1

    payload = {"at": resolved_now.isoformat(), "by": controller, "lease_seconds": LEASE_SECONDS}
    created = write_phase(log_dir, task_id, family, n, "req", payload)
    return Attempt(n=n, owned=created)


def cancel_verdict(log_dir: Path, task_id: str, now: datetime | None = None) -> str:
    """Whether a cross-process cancel is authorized for `task_id`.

    `"none"` — no cancel has ever been attempted. `"authorized"` — the latest attempt's signal was
    delivered to a still-alive leader (so the exit that follows should be read as `cancelled`).
    `"not_authorized"` — the latest attempt signalled a leader that was already gone, gave up
    outright, or its controller was found abandoned (which this call also recovers, publishing
    `.failed`, so a caller checking this does not need to call `recover_abandoned` itself).
    `"pending"` — an attempt is underway with a controller that is still live or undecidable, or
    it has a `.failed` but a joining controller's delivery is still outstanding (`attempt_state`).
    """
    resolved_now = now or datetime.now(timezone.utc)
    latest = latest_attempt(log_dir, task_id, CANCEL)
    if latest is None:
        return "none"
    if attempt_outcome(log_dir, task_id, CANCEL, latest) == "pending":
        recover_abandoned(log_dir, task_id, CANCEL, latest, resolved_now)
    state = attempt_state(log_dir, task_id, CANCEL, latest, resolved_now)
    if state == "sig":
        payload = read_phase(log_dir, task_id, CANCEL, latest, "sig")
        if isinstance(payload, dict) and payload.get("leader_alive") is True:
            return "authorized"
        return "not_authorized"
    if state == "failed":
        return "not_authorized"
    return "pending"


def mark_signalled(
    log_dir: Path, task_id: str, family: str, n: int, *, leader_alive: bool, now: datetime | None = None
) -> bool:
    resolved_now = now or datetime.now(timezone.utc)
    return write_phase(
        log_dir, task_id, family, n, "sig", {"at": resolved_now.isoformat(), "leader_alive": leader_alive}
    )


def mark_failed(
    log_dir: Path, task_id: str, family: str, n: int, *, reason: str, now: datetime | None = None
) -> bool:
    resolved_now = now or datetime.now(timezone.utc)
    return write_phase(log_dir, task_id, family, n, "failed", {"at": resolved_now.isoformat(), "reason": reason})


def cancel_attempt_active(log_dir: Path, task_id: str, now: datetime | None = None) -> bool:
    """For retention: whether `task_id` has a cancel attempt still worth keeping the task for.

    Only a *pending* latest attempt with a controller that is not (yet, provably) abandoned counts
    as active — a signalled, failed, or abandoned attempt is done and the task may age out. This
    never itself recovers an abandoned attempt (unlike `cancel_verdict`); a sweep only needs to know
    whether to keep the task, not to mutate control state on the way.
    """
    resolved_now = now or datetime.now(timezone.utc)
    latest = latest_attempt(log_dir, task_id, CANCEL)
    if latest is None:
        return False
    if attempt_state(log_dir, task_id, CANCEL, latest, resolved_now) != "pending":
        return False
    if attempt_outcome(log_dir, task_id, CANCEL, latest) == "failed":
        return True  # a joiner's delivery is still outstanding
    req = read_phase(log_dir, task_id, CANCEL, latest, "req")
    return not controller_abandoned(req, resolved_now)


def record_lock_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}.lock"


def session_lock_path(log_dir: Path, session_id: str) -> Path:
    """A lock file per session, named directly by the session id when that is filesystem-safe, else
    by a hash of it — a session id is caller-supplied and may contain anything."""
    name = session_id if _SAFE_SESSION_ID_RE.match(session_id or "") else _hash_session_id(session_id)
    return log_dir.parent / "sessions" / f"{name}.lock"


def _hash_session_id(session_id: str) -> str:
    return "h-" + hashlib.sha256(str(session_id).encode("utf-8")).hexdigest()[:32]


async def acquire(path: Path, timeout: float) -> int:
    """Take an exclusive `flock` on `path`, creating it if needed. Never blocks the event loop: a
    contended lock is retried on a short `asyncio.sleep`, not a blocking wait."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o644)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise LockTimeout(f"timed out acquiring lock {path}") from None
                await asyncio.sleep(_LOCK_POLL_SECONDS)
    except BaseException:
        # Including a cancellation during the retry sleep: the caller's `finally` never received
        # this descriptor, so nobody else can close it.
        os.close(fd)
        raise


def release(fd: int) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass
    os.close(fd)


@asynccontextmanager
async def session_lock(log_dir: Path, session_id: str, timeout: float = 30.0):
    """Serialises `resume`/`resume_record` for one session across every server process — held
    across the check-and-spawn it protects, not just the check; see the plan's Notes on why that is
    fine even though nothing else here awaits while holding a lock."""
    fd = await acquire(session_lock_path(log_dir, session_id), timeout)
    try:
        yield
    finally:
        release(fd)


def write_record_if_open(
    log_dir: Path,
    task_id: str,
    update: Callable[[store.TaskRecord], store.TaskRecord],
    *,
    timeout: float = RECORD_LOCK_TIMEOUT_SECONDS,
) -> store.TaskRecord | None:
    """`close_record_if_open`, returning the written record or None if nothing was written."""
    outcome, record = close_record_if_open(log_dir, task_id, update, timeout=timeout)
    return record if outcome == "written" else None


def close_record_if_open(
    log_dir: Path,
    task_id: str,
    update: Callable[[store.TaskRecord], store.TaskRecord],
    *,
    timeout: float = RECORD_LOCK_TIMEOUT_SECONDS,
) -> tuple[str, store.TaskRecord | None]:
    """Apply `update` to `task_id`'s record, but only if it is still open for a non-owner to act on.

    Synchronous and meant to run via `asyncio.to_thread` — nothing here awaits, so blocking with a
    plain `time.sleep` retry between `flock` attempts is fine; the calling coroutine is the one that
    yields, by virtue of being in a worker thread. Only a *dead* owner's record is safe for someone
    else to close: `alive` and `undecidable` both refuse.

    Returns `(outcome, record)`: `"written"` with the new record; `"terminal"` with the existing
    one (something already settled it); `"missing"`; `"owner_not_dead"`; or `"write_failed"` when
    the write did not land on disk. Raises `LockTimeout` if the lock cannot be taken.
    """
    fd = os.open(record_lock_path(log_dir, task_id), os.O_CREAT | os.O_RDWR, 0o644)
    deadline = time.monotonic() + timeout
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise LockTimeout(f"timed out acquiring record lock for {task_id}") from None
                time.sleep(_LOCK_POLL_SECONDS)

        record = store.read(log_dir, task_id)
        if record is None:
            return "missing", None
        if record.status in store.TERMINAL_RECORD_STATUSES:
            return "terminal", record
        if identity.identity_check(record.owner) != "dead":
            return "owner_not_dead", None
        updated = update(record)
        if not store.write_landed(log_dir, updated):
            return "write_failed", None
        return "written", updated
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(fd)
