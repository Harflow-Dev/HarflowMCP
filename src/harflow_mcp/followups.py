"""Queued next-continuation follow-ups for tasks without live input.

A task that cannot take a message while it runs (a backend without live input, or claude with a
turn cap) can still be told something for later. `send_message` appends the text to a durable
per-task ledger, `<task_id>.followups.json`, and answers `queued` with `delivery:
"next_continuation"` — nothing is started and nothing is delivered. The ledger is consumed, exactly
once, by the next continuation a caller starts explicitly from that task (`resume_task`,
`continue_task`, `harflow-ctl resume|continue`). This is a separate store from the live-input
inbox (`inbox.py`): it never touches the inbox, its lock or its close protocol.

Rules that keep an acknowledged message from being lost or duplicated:

* **One lock, `<id>.followups.lock`** — empty, a permanent `flock` target, never deleted (retention
  leaves it in place). Every operation takes it; nothing awaits while it is held. The async
  entry points run the blocking work in a worker thread.
* **The state is one bounded JSON document**, replaced atomically under that lock (temp file,
  fsync, rename, directory fsync). There are no torn lines and no unbounded history. A message is
  at most 32 KiB of UTF-8; admission refuses (`queue_full`) once the serialized document would
  pass 960 KiB or there would be more than 16 outstanding items, which leaves headroom under the
  1 MiB read budget for internal transitions. A document that cannot be read within budget or fails
  validation reads as `unavailable` and is never guessed at.
* **Statuses**: `queued`, `reserved` (handed to a successor that is being started), `consumed`
  (final), `cancelled` (final), `undeliverable` and `delivery_unknown` (both kept, with their text).
  Final items keep metadata only, the newest 20, counting the rest in `omitted_final`.
* **Sealing.** A ledger refuses new messages (`superseded`, naming the successor) once a successor
  exists or probably exists: `reserved` seals it and a `released` attempt unseals it, while
  `consumed`, `delivery_unknown` and `superseded` seal it permanently. A message can therefore
  never land on a source that already has, or probably has, a successor.
* **Consumption happens only on explicitly caller-initiated paths** and follows a fixed sequence:
  reserve every queued message for the successor id (sealing), compose the prompt, build and spawn,
  then write `consumed`. A failure decides `released` (provably not started), `consumed` (the
  successor exists) or `delivery_unknown` (it may have reached an agent). A failed terminal write
  never changes the launch outcome: this process remembers it and retries the next time it takes
  that task's follow-up lock, and read-time resolution reports the truth meanwhile.

Lock order: the session lock (or the continuation-chain lock), then this task's follow-up lock,
never the reverse. Retention takes its catalog locks and then the follow-up lock, and never a
session lock while holding it.
"""

from __future__ import annotations

from .state_access import task_operation, task_layout, assert_task_owned, open_descriptor, layout_operation
from .state_ownership import owned_job, run_owned_worker, StateOwnershipError

from .state_layout import resolve_layout

import asyncio
import contextlib
import fcntl
import json
import logging
import os
import stat
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator

from . import control, identity, inbox, store, followup_documents
from .catalog import source_identity

log = logging.getLogger(__name__)

STATE_SUFFIX = ".followups.json"
LOCK_SUFFIX = ".followups.lock"

MAX_MESSAGE_BYTES = 32 * 1024
MAX_OUTSTANDING = 16
MAX_FINAL = 20
ADMISSION_BUDGET_BYTES = 960 * 1024
READ_BUDGET_BYTES = 1024 * 1024
LOCK_TIMEOUT_SECONDS = 10.0

# How long a status read may reuse a successor scan. Sends and reservations always scan afresh.
SUCCESSOR_CACHE_SECONDS = 5.0
SUCCESSOR_CACHE_LIMIT = 256

QUEUED, RESERVED, CONSUMED, CANCELLED = "queued", "reserved", "consumed", "cancelled"
UNDELIVERABLE, DELIVERY_UNKNOWN = "undeliverable", "delivery_unknown"
RELEASED = "released"
STATUSES = followup_documents.STATUSES
FINAL_STATUSES = frozenset({CONSUMED, CANCELLED})
OUTSTANDING_STATUSES = STATUSES - FINAL_STATUSES

# Ends a composed prompt that has no explicit prompt, so it never ends in caller whitespace
# (vibe refuses a large prompt with outer whitespace).
CLOSING_LINE = "End of queued caller follow-ups."

QUEUED_NOTE = (
    "queued for the next continuation: nothing was started and nothing was delivered. It is "
    "consumed, exactly once, by the next continuation you start explicitly from this task "
    "(resume_task, continue_task, harflow-ctl resume or continue, or Resume in the Monitor). "
    "Cancel it with cancel_queued_message while it is still queued."
)


class Unavailable(Exception):
    """The follow-up document cannot be read within budget or is not valid."""


class _TooLarge(Exception):
    """A document that would pass the byte limit it is being written under."""


def state_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{STATE_SUFFIX}"


def lock_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{LOCK_SUFFIX}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# --- process-local bookkeeping ----------------------------------------------------------------

# Successor ids whose launch this process is still working on, keyed by (log dir, successor id).
# A `reserved` item whose successor is in here is in progress, not abandoned.
_INFLIGHT: set[tuple[str, str]] = set()
# Terminal writes that failed while this process lives, retried the next time it takes that
# task's follow-up lock: (log dir, source task id) -> [(successor id, message ids, outcome, reason)].
_PENDING: dict[tuple[str, str], list[tuple[str, list[str], str, str | None]]] = {}
_PENDING_GUARD = threading.Lock()
_SUCCESSOR_CACHE: dict[tuple[str, str], tuple[float, list[str]]] = {}
# Test seam: called by `hold_for_delete` right after it takes the lock.
_after_delete_lock: Callable[[], None] | None = None


def _key(log_dir: Path, name: str) -> tuple[str, str]:
    return str(log_dir), name


# --- document ---------------------------------------------------------------------------------

def _empty() -> dict[str, Any]:
    return {"v": 1, "next_seq": 1, "sealed_by": None, "items": [], "omitted_final": 0}


class _Document(dict):
    """Inner ledger with private, nonserialized source provenance."""

    def __init__(self, ledger, task_id, source, envelope=None, *, path):
        super().__init__(ledger)
        self.task_id, self.source, self.envelope = task_id, source, envelope
        self.path = path.absolute()


def _valid(doc: Any) -> bool:
    try:
        followup_documents.validate(doc)
        return True
    except (ValueError, TypeError, RecursionError):
        return False


def _source(path: Path):
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise Unavailable("follow-up source cannot be inspected") from exc
    if not stat.S_ISREG(info.st_mode):
        raise Unavailable("follow-up source is not a regular file")
    return source_identity(info)


def _check_source(path: Path, doc: dict[str, Any]) -> None:
    if not isinstance(doc, _Document) or path.absolute() != doc.path or _source(path) != doc.source:
        raise Unavailable("follow-up source changed before replacement")


def _load(log_dir: Path, task_id: str) -> dict[str, Any]:
    """Qualify a bounded source, or capture verified initial absence."""
    assert_task_owned(log_dir)
    path = state_path(log_dir, task_id)
    descriptor = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise Unavailable("follow-up source is not a regular file")
        source = source_identity(opened)
        if opened.st_size > READ_BUDGET_BYTES:
            raise Unavailable("follow-up source exceeds its read budget")
        with os.fdopen(os.dup(descriptor), "rb") as handle:
            data = handle.read(READ_BUDGET_BYTES + 1)
        if len(data) > READ_BUDGET_BYTES:
            raise Unavailable("follow-up source exceeds its read budget")
        if source_identity(os.fstat(descriptor)) != source or _source(path) != source:
            raise Unavailable("follow-up source changed during read")
        envelope = json.loads(data.decode("utf-8"))
        ledger = followup_documents.decode(envelope, task_id)
        return _Document(ledger, task_id, source, envelope, path=path)
    except FileNotFoundError as exc:
        if descriptor is None and _source(path) is None:
            return _Document(_empty(), task_id, None, path=path)
        raise Unavailable("follow-up source disappeared during read") from exc
    except (OSError, ValueError, UnicodeError, TypeError, RecursionError,
            followup_documents.FollowupDocumentError) as exc:
        raise Unavailable(f"the follow-up ledger of task {task_id} is unavailable") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _serialize(doc: dict[str, Any], task_id: str | None = None) -> bytes:
    try:
        task_id = task_id if task_id is not None else doc.task_id
        envelope = followup_documents.encode(doc, task_id, envelope=getattr(doc, "envelope", None))
        return json.dumps(envelope, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (AttributeError, ValueError, TypeError, RecursionError, UnicodeError,
            followup_documents.FollowupDocumentError) as exc:
        raise Unavailable("follow-up candidate failed qualification") from exc


def _final(item: dict[str, Any], status: str, at: str) -> dict[str, Any]:
    """The metadata-only form a consumed or cancelled item keeps."""
    kept = {"id": item["id"], "seq": item["seq"], "status": status, "at": at}
    if item.get("successor_task_id"):
        kept["successor_task_id"] = item["successor_task_id"]
    for key in ("target", "delivery"):
        if key in item:
            kept[key] = item[key]
    return kept


def _compact(doc: dict[str, Any]) -> None:
    """Keep the newest `MAX_FINAL` final items, counting the ones dropped."""
    finals = [item for item in doc["items"] if item["status"] in FINAL_STATUSES]
    if len(finals) <= MAX_FINAL:
        return
    finals.sort(key=lambda item: item["seq"])
    dropped = {item["id"] for item in finals[: len(finals) - MAX_FINAL]}
    doc["items"] = [item for item in doc["items"] if item["id"] not in dropped]
    doc["omitted_final"] += len(dropped)


def _store(log_dir: Path, task_id: str, doc: dict[str, Any], limit: int = READ_BUDGET_BYTES) -> None:
    """Replace a source-bound qualified document; refuse changed or unqualified sources."""
    assert_task_owned(log_dir)
    store.read(log_dir, task_id, include_prompt=False)
    if not isinstance(doc, _Document) or doc.task_id != task_id:
        raise Unavailable("follow-up candidate has no source provenance")
    if not _valid(doc):
        raise Unavailable("follow-up candidate failed qualification")
    _compact(doc)
    data = _serialize(doc, task_id)
    if len(data) > limit:
        raise _TooLarge()
    path = state_path(log_dir, task_id)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    created = False
    try:
        with temporary.open("xb") as handle:
            created = True
            os.fchmod(handle.fileno(), 0o600)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
            replacement = source_identity(os.fstat(handle.fileno()))
        _check_source(path, doc)
        os.replace(temporary, path)
        doc.source = replacement
        control._fsync_dir(log_dir)
    finally:
        if created:
            temporary.unlink(missing_ok=True)


# --- locking ----------------------------------------------------------------------------------

@contextlib.contextmanager
def _locked(log_dir: Path, task_id: str, timeout: float = LOCK_TIMEOUT_SECONDS) -> Iterator[None]:
    """The follow-up lock, with any terminal write this process still owes for the task retried
    first. Raises `control.LockTimeout`."""
    assert_task_owned(log_dir)
    with control._flock_sync(lock_path(log_dir, task_id), timeout, f"follow-up lock for {task_id}", layout=resolve_layout(log_dir=log_dir)):
        _retry_pending(log_dir, task_id)
        yield


def _retry_pending(log_dir: Path, task_id: str) -> None:
    assert_task_owned(log_dir)
    key = _key(log_dir, task_id)
    with _PENDING_GUARD:
        pending = list(_PENDING.get(key, ()))
    if not pending:
        return
    try:
        doc = _load(log_dir, task_id)
        for successor, ids, outcome, reason in pending:
            _apply_terminal(doc, successor, ids, outcome, reason)
        _store(log_dir, task_id, doc)
    except (Unavailable, _TooLarge, OSError, store.TaskDocumentError):
        log.warning("task %s: pending follow-up terminal write still cannot land", task_id, exc_info=True)
        return
    with _PENDING_GUARD:
        _PENDING.pop(key, None)


# --- read-time resolution -----------------------------------------------------------------------

def _is_this_process(owner: Any) -> bool:
    if not isinstance(owner, dict) or owner.get("pid") != os.getpid():
        return False
    try:
        mine = identity.own_identity().get("start_time")
    except StateOwnershipError:
        raise
    except Exception:
        mine = None
    theirs = owner.get("start_time")
    return mine is None or theirs is None or mine == theirs


def _launch_verdict(log_dir: Path, successor: str) -> str | None | bool:
    """What the successor's launch receipt says, for a launch that may be waiting for resources.

    `False` means the receipt has nothing to add (no receipt, or one that is not in a resource
    state) and the ordinary owner rules apply. Otherwise the same answers as `_reserved_verdict`:
    a waiting or granted launch whose owner is alive or cannot be inspected is in progress (None);
    one that never started releases the items back to the queue; a granted launch whose owner is
    gone with no record may already have spawned, so the delivery is unknown.
    """
    from .task_supervision import read_launch
    try:
        receipt = read_launch(log_dir, successor)
    except (store.TaskDocumentError, OSError):
        return False
    if receipt is None:
        return False
    if receipt.get("followups_delivery_unknown") and (receipt["state"] in ("settled", "not_started") or receipt.get("recovery_intent")):
        return DELIVERY_UNKNOWN
    if not (isinstance(receipt.get("resources"), dict) or isinstance(receipt.get("worktree"), dict)):
        return False
    state = receipt["state"]
    if state == "not_started":
        return DELIVERY_UNKNOWN if receipt.get("followups_delivery_unknown") else RELEASED
    if state not in ("waiting_for_resources", "granted"):
        return False
    try:
        owner_verdict = identity.identity_check(receipt.get("owner"))
    except StateOwnershipError:
        raise
    except Exception:
        owner_verdict = "undecidable"
    if owner_verdict != "dead":
        return None
    # The probe can be slow and the owner may have granted, spawned and died meanwhile, so the
    # receipt and the record are read again now that it is confirmed gone; nothing changes after.
    try:
        fresh = read_launch(log_dir, successor)
        if store.read(log_dir, successor, include_prompt=False) is not None:
            return CONSUMED
    except (store.TaskDocumentError, OSError):
        return None
    if fresh is None:
        return DELIVERY_UNKNOWN
    if fresh["state"] == "not_started" and fresh.get("followups_delivery_unknown"):
        return DELIVERY_UNKNOWN
    return RELEASED if fresh["state"] in ("waiting_for_resources", "not_started") else DELIVERY_UNKNOWN


def _reserved_verdict(log_dir: Path, item: dict[str, Any]) -> str | None:
    """What a `reserved` item has become, or None while it is still genuinely in progress."""
    assert_task_owned(log_dir)
    successor = item.get("successor_task_id")
    if not isinstance(successor, str):
        return DELIVERY_UNKNOWN
    launch_verdict = _launch_verdict(log_dir, successor)
    if launch_verdict == DELIVERY_UNKNOWN:
        return DELIVERY_UNKNOWN
    try:
        landed = store.read(log_dir, successor, include_prompt=False) is not None
    except store.InvalidTaskId:
        return DELIVERY_UNKNOWN
    if landed:
        return CONSUMED
    if _key(log_dir, successor) in _INFLIGHT:
        return None
    launched = _launch_verdict(log_dir, successor)
    if launched is not False:
        return launched
    owner = item.get("owner")
    if _is_this_process(owner):
        return DELIVERY_UNKNOWN
    try:
        if identity.identity_check(owner) == "dead":
            return DELIVERY_UNKNOWN
    except StateOwnershipError:
        raise
    except Exception:
        pass
    return None  # another live (or unprovable) owner: truthfully in progress or unresolved


@task_operation
def successor_candidates(log_dir: Path, record: store.TaskRecord, *, cached: bool = False) -> list[str]:
    """Same-session successors and continuations of `record`, oldest first."""
    key = _key(log_dir, record.task_id)
    # Requalify history before using a result; cached absence is not current authority.
    records = store.scan(log_dir).require_complete()
    found: set[str] = set()
    if record.session_id:
        found |= store.session_successor_task_ids(log_dir, record.task_id, record.session_id)
    found |= {r.task_id for r in records if r.continuation_of_task_id == record.task_id}
    found.discard(record.task_id)

    def started(identifier: str) -> tuple[str, str]:
        other = store.read(log_dir, identifier)
        return (other.started_at if other is not None and other.started_at else "", identifier)

    ordered = sorted(found, key=started)
    if cached:
        now = time.monotonic()
        for stale in [k for k, (at, _) in _SUCCESSOR_CACHE.items() if now - at >= SUCCESSOR_CACHE_SECONDS]:
            del _SUCCESSOR_CACHE[stale]
        while len(_SUCCESSOR_CACHE) >= SUCCESSOR_CACHE_LIMIT:
            del _SUCCESSOR_CACHE[next(iter(_SUCCESSOR_CACHE))]
        _SUCCESSOR_CACHE[key] = (now, ordered)
    return ordered


def _resolve(
    log_dir: Path, record: store.TaskRecord | None, doc: dict[str, Any], successors: Callable[[], list[str]] | None
) -> bool:
    """Apply the read-time rules to `doc` in place; whether anything changed. These only repair
    what a crash or a failed terminal write left behind."""
    assert_task_owned(log_dir)
    store.scan(log_dir).require_complete()
    changed = False
    now = _now()
    verdicts: dict[str, str] = {}
    for item in doc["items"]:
        if item["status"] != RESERVED:
            continue
        successor = item.get("successor_task_id")
        if successor not in verdicts:
            verdict = _reserved_verdict(log_dir, item)
            if verdict is None:
                continue
            verdicts[successor] = verdict
        verdict = verdicts[successor]
        changed = True
        if verdict == CONSUMED:
            doc["items"][doc["items"].index(item)] = _final(item, CONSUMED, now)
            doc["sealed_by"] = successor
        elif verdict == RELEASED:
            # The successor provably never started: the messages are queued again.
            item["status"] = QUEUED
            for stale in ("successor_task_id", "owner", "at", "reason"):
                item.pop(stale, None)
            if doc.get("sealed_by") == successor:
                doc["sealed_by"] = None
        else:
            item.update(status=DELIVERY_UNKNOWN, at=now, reason=(
                f"the continuation {successor} was being started when its owner stopped or its "
                "launch could not be recorded, and it left no task; the messages may have reached "
                "an agent, so inspect before resending"))
            item.pop("owner", None)
            doc["sealed_by"] = successor
    if successors is not None and any(item["status"] == QUEUED for item in doc["items"]):
        later = successors()
        if later:
            newest = later[-1]
            for item in doc["items"]:
                if item["status"] == QUEUED:
                    item.update(status=UNDELIVERABLE, at=now, reason=f"superseded by {newest}; resend to it")
            doc["sealed_by"] = newest
            changed = True
    return changed


def _apply_terminal(
    doc: dict[str, Any], successor: str, ids: list[str], outcome: str, reason: str | None
) -> None:
    """Move this attempt's `reserved` items to their terminal state. Idempotent: an item already
    resolved by something else is left as it is."""
    now = _now()
    wanted = set(ids)
    for index, item in enumerate(doc["items"]):
        if item["id"] not in wanted or item["status"] != RESERVED or item.get("successor_task_id") != successor:
            continue
        if outcome == CONSUMED:
            doc["items"][index] = _final(item, CONSUMED, now)
        elif outcome == DELIVERY_UNKNOWN:
            item.update(status=DELIVERY_UNKNOWN, at=now, reason=reason)
            item.pop("owner", None)
        else:  # released: provably not started, queued again
            item["status"] = QUEUED
            for stale in ("successor_task_id", "owner", "at", "reason"):
                item.pop(stale, None)
    if outcome == "released":
        if doc.get("sealed_by") == successor:
            doc["sealed_by"] = None
    else:
        doc["sealed_by"] = successor


def _lazy(compute: Callable[[], list[str]]) -> Callable[[], list[str]]:
    memo: list[list[str]] = []

    def get() -> list[str]:
        if not memo:
            memo.append(compute())
        return memo[0]

    return get


# --- sending and cancelling ---------------------------------------------------------------------

def _refused_superseded(task_id: str, successor: str) -> inbox.SendRefused:
    return inbox.SendRefused(
        f"task {task_id} already has a successor ({successor}), so a message for it would never be "
        f"consumed; send the message to {successor} instead",
        code="superseded",
    )


def _guard(log_dir: Path, task_id: str) -> None:
    assert_task_owned(log_dir)
    from .workflow_hooks import refuse_direct_message
    refuse_direct_message(log_dir, task_id)


@task_operation
def send(
    log_dir: Path, task_id: str, text: str, *, by: dict[str, Any] | None,
    timeout: float = LOCK_TIMEOUT_SECONDS, allow_live: bool = False, delivery: str = "queue",
) -> dict[str, Any]:
    """Queue `text` for the next continuation of a task without live input. Blocking: run it in a
    worker thread, or from a CLI. Refusals raise `inbox.SendRefused` with a stable `code`."""
    store.validate_task_id(task_id)
    _guard(log_dir, task_id)
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError:
        raise inbox.SendRefused("the message is not valid UTF-8 text", code="invalid_text") from None
    if size > MAX_MESSAGE_BYTES:
        raise inbox.SendRefused(
            f"the message is {size} bytes; at most {MAX_MESSAGE_BYTES} are queued per message — "
            "split it, or put the detail in a file and reference it",
            code="message_too_large",
        )
    record = store.read(log_dir, task_id)
    if record is None:
        raise inbox.SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
    if record.live_input and not allow_live:
        raise inbox.SendRefused(
            f"task {task_id} has live input; its messages go to the running agent, not a ledger",
            code="live_input",
        )
    if record.workflow_builder:
        raise inbox.SendRefused(
            f"task {task_id} belongs to a workflow builder; respond through the workflow caller",
            code="workflow_owned",
        )
    try:
        with _locked(log_dir, task_id, timeout):
            record = store.read(log_dir, task_id)
            if record is None:
                raise inbox.SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
            try:
                doc = _load(log_dir, task_id)
            except Unavailable as exc:
                raise inbox.SendRefused(str(exc), code="unavailable") from exc
            later = _lazy(lambda: successor_candidates(log_dir, record))
            changed = _resolve(log_dir, record, doc, later)
            if changed:
                _persist(log_dir, task_id, doc)
            if doc["sealed_by"]:
                raise _refused_superseded(task_id, doc["sealed_by"])
            if later():
                raise _refused_superseded(task_id, later()[-1])
            outstanding = sum(1 for item in doc["items"] if item["status"] in OUTSTANDING_STATUSES)
            if outstanding >= MAX_OUTSTANDING:
                raise inbox.SendRefused(
                    f"task {task_id} already has {outstanding} outstanding follow-ups; cancel some "
                    "or continue the task first",
                    code="queue_full",
                )
            item = {
                "id": uuid.uuid4().hex, "seq": doc["next_seq"], "status": QUEUED,
                "text": text, "queued_at": _now(), "by": by,
                "delivery": delivery, "target": {"kind": "task", "task_id": task_id},
            }
            doc["next_seq"] += 1
            doc["items"].append(item)
            try:
                _store(log_dir, task_id, doc, ADMISSION_BUDGET_BYTES)
            except _TooLarge:
                raise inbox.SendRefused(
                    f"task {task_id}'s queued follow-ups would exceed the ledger's size budget; "
                    "cancel some or continue the task first",
                    code="queue_full",
                ) from None
            except OSError as exc:
                raise inbox.SendRefused(
                    f"could not queue the message for task {task_id}: {exc}", code="write_failed"
                ) from exc
    except Unavailable as exc:
        raise inbox.SendRefused(str(exc), code="unavailable") from exc
    except control.LockTimeout:
        raise inbox.SendRefused(
            f"task {task_id}'s follow-up queue is locked by another process; try again shortly",
            code="lock_timeout",
        ) from None
    return {
        "task_id": task_id, "message_id": item["id"], "seq": item["seq"], "status": QUEUED,
        "delivery": "next_continuation", "note": QUEUED_NOTE,
    }


def _persist(log_dir: Path, task_id: str, doc: dict[str, Any]) -> None:
    """Write resolution changes. A failure is not an outcome: the next lock holder repeats them."""
    assert_task_owned(log_dir)
    try:
        _store(log_dir, task_id, doc)
    except (_TooLarge, OSError):
        log.warning("task %s: could not persist follow-up resolution", task_id, exc_info=True)


@task_operation
def dispatch_send(
    log_dir: Path, task_id: str, text: str, *, by: dict[str, Any] | None,
    timeout: float = LOCK_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """The one send route for callers with no registry (`harflow-ctl send`): a live-input task
    goes to its inbox, any other standalone task to the follow-up ledger. The refusals of both are
    `inbox.SendRefused`."""
    record = store.read(log_dir, store.validate_task_id(task_id))
    if record is not None and not record.live_input:
        return send(log_dir, task_id, text, by=by, timeout=timeout)
    return inbox.send_to_record(log_dir, task_id, text, by=by, timeout=timeout)


@task_operation
def cancel(
    log_dir: Path, task_id: str, message_id: str, *, timeout: float = LOCK_TIMEOUT_SECONDS
) -> dict[str, Any]:
    """Cancel one still-`queued` message. Blocking. Refusals raise `inbox.SendRefused`."""
    store.validate_task_id(task_id)
    _guard(log_dir, task_id)
    if store.read(log_dir, task_id) is None:
        raise inbox.SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
    try:
        with _locked(log_dir, task_id, timeout):
            record = store.read(log_dir, task_id)
            if record is None:
                raise inbox.SendRefused(f"unknown task_id: {task_id}", code="unknown_task")
            try:
                doc = _load(log_dir, task_id)
            except Unavailable as exc:
                raise inbox.SendRefused(str(exc), code="unavailable") from exc
            if _resolve(log_dir, record, doc, _lazy(lambda: successor_candidates(log_dir, record))):
                _persist(log_dir, task_id, doc)
            for index, item in enumerate(doc["items"]):
                if item["id"] != message_id:
                    continue
                if item["status"] != QUEUED:
                    raise inbox.SendRefused(
                        f"message {message_id} is {item['status']}, not queued, so it cannot be "
                        "cancelled", code="not_cancellable",
                    )
                doc["items"][index] = _final(item, CANCELLED, _now())
                try:
                    _store(log_dir, task_id, doc)
                except (_TooLarge, OSError) as exc:
                    raise inbox.SendRefused(
                        f"could not cancel message {message_id}: {exc}", code="write_failed"
                    ) from exc
                return {"task_id": task_id, "message_id": message_id, "status": CANCELLED}
            raise inbox.SendRefused(
                f"task {task_id} has no message {message_id}; it may be older than the retained "
                "history", code="unknown_message",
            )
    except Unavailable as exc:
        raise inbox.SendRefused(str(exc), code="unavailable") from exc
    except control.LockTimeout:
        raise inbox.SendRefused(
            f"task {task_id}'s follow-up queue is locked by another process; try again shortly",
            code="lock_timeout",
        ) from None


# --- status -------------------------------------------------------------------------------------

_UNAVAILABLE_VIEW = {"state": "unavailable", "items": [], "omitted_final": 0}


def _present(doc: dict[str, Any]) -> dict[str, Any]:
    items = []
    for item in sorted(doc["items"], key=lambda entry: entry["seq"]):
        if item["status"] in FINAL_STATUSES:
            shown = {k: item[k] for k in ("id", "seq", "status", "successor_task_id", "at") if k in item}
        else:
            shown = {"id": item["id"], "seq": item["seq"], "status": item["status"],
                     "text": item["text"], "queued_at": item.get("queued_at")}
            for extra in ("successor_task_id", "reason"):
                if item.get(extra):
                    shown[extra] = item[extra]
        for key in ("delivery", "target"):
            if key in item:
                shown[key] = item[key]
        items.append(shown)
    return {"state": "available", "items": items, "omitted_final": doc["omitted_final"]}


@task_operation
def view(log_dir: Path, task_id: str) -> dict[str, Any]:
    """`queued_followups` for a status document. Blocking file reads: run it off the event loop.
    Ownership failures raise; unreadable ledgers are reported without writes: terminal writes this
    process still owes are applied to what is shown, and resolutions this read observes are
    persisted by the next lock holder (send, cancel, a consuming continuation, retention)."""
    try:
        doc = _load(log_dir, task_id)
        if doc.source is None:
            return _present(doc)
        with _PENDING_GUARD:
            owed = list(_PENDING.get(_key(log_dir, task_id), ()))
        for successor, ids, outcome, reason in owed:
            _apply_terminal(doc, successor, ids, outcome, reason)
        record = store.read(log_dir, task_id)
        if record is not None:
            _resolve(log_dir, record, doc, _lazy(lambda: successor_candidates(log_dir, record, cached=True)))
        return _present(doc)
    except store.TaskDocumentError as exc:
        return dict(_UNAVAILABLE_VIEW) | {"history_incomplete": True, "refused_tasks": [exc.as_dict()]}
    except Unavailable:
        return dict(_UNAVAILABLE_VIEW)
    except StateOwnershipError:
        raise
    except Exception:
        log.warning("task %s: follow-up status unavailable", task_id, exc_info=True)
        return dict(_UNAVAILABLE_VIEW)


def count_outstanding(view_document: dict[str, Any]) -> int | None:
    """Outstanding items in a `view` document, None when it is unavailable."""
    if view_document.get("state") != "available":
        return None
    return sum(1 for item in view_document["items"] if item["status"] in OUTSTANDING_STATUSES)


@task_operation
def outstanding_count(log_dir: Path, task_id: str) -> int | None:
    """Outstanding items for a listing: 0 with no ledger, None when it cannot be read. Cheap, and
    without the read-time rules."""
    try:
        doc = _load(log_dir, task_id)
    except (Unavailable, store.InvalidTaskId):
        return None
    return sum(1 for item in doc["items"] if item["status"] in OUTSTANDING_STATUSES)


# --- retention ----------------------------------------------------------------------------------

@task_operation
def reconcile_all(log_dir: Path) -> None:
    """Persist read-time resolutions for every task with a ledger, so retention protects only what
    is genuinely outstanding. Each ledger is taken without waiting: a busy one is skipped and stays
    protected this sweep. Failures are bookkeeping, never an outcome."""
    try:
        paths = list(log_dir.glob(f"*{STATE_SUFFIX}"))
    except OSError:
        return
    for path in paths:
        task_id = path.name[: -len(STATE_SUFFIX)]
        try:
            store.validate_task_id(task_id)
            with _locked(log_dir, task_id, timeout=0):
                record = store.read(log_dir, task_id)
                if record is None:
                    continue
                doc = _load(log_dir, task_id)
                if _resolve(log_dir, record, doc, _lazy(lambda: successor_candidates(log_dir, record))):
                    _persist(log_dir, task_id, doc)
        except (control.LockTimeout, Unavailable, store.InvalidTaskId, OSError, store.TaskDocumentError):
            continue
        except StateOwnershipError:
            raise
        except Exception:
            log.warning("task %s: follow-up reconciliation failed", path.name, exc_info=True)


@task_operation
def protected_task_ids(log_dir: Path) -> set[str]:
    """Tasks retention must keep: those with queued or reserved items, the successors a
    reservation names, and any whose ledger cannot be read (unreadable is not provably empty)."""
    protected: set[str] = set()
    try:
        paths = list(log_dir.glob(f"*{STATE_SUFFIX}"))
    except OSError:
        return protected
    for path in paths:
        task_id = path.name[: -len(STATE_SUFFIX)]
        try:
            store.validate_task_id(task_id)
        except store.InvalidTaskId:
            continue
        try:
            doc = _load(log_dir, task_id)
        except Unavailable:
            protected.add(task_id)
            continue
        for item in doc["items"]:
            if item["status"] in (QUEUED, RESERVED):
                protected.add(task_id)
            if item["status"] == RESERVED and isinstance(item.get("successor_task_id"), str):
                protected.add(item["successor_task_id"])
    return protected


@contextlib.contextmanager
@task_operation
def hold_for_delete(log_dir: Path, task_id: str) -> Iterator[str]:
    """Hold the follow-up lock across a task's deletion. Yields `ok` when it may go ahead, `busy`
    when the lock is taken, `blocked` when something is queued or reserved (or the ledger cannot be
    read). The caller must delete the state file and the record while inside, and leaves the lock
    file in place."""
    try:
        fd = open_descriptor(lock_path(log_dir, task_id), os.O_CREAT | os.O_RDWR, 0o644, layout=task_layout(log_dir))
    except OSError:
        yield "busy"
        return
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            yield "busy"
            return
        if _after_delete_lock is not None:
            _after_delete_lock()
        try:
            doc = _load(log_dir, task_id)
            record = store.read(log_dir, task_id)
            _resolve(log_dir, record, doc, _lazy(lambda: successor_candidates(log_dir, record))
                     if record is not None else None)
        except Unavailable:
            yield "blocked"
            return
        if any(item["status"] in (QUEUED, RESERVED) for item in doc["items"]):
            yield "blocked"
            return
        try:
            _check_source(state_path(log_dir, task_id), doc)
        except Unavailable:
            yield "blocked"
            return
        yield "ok"
    finally:
        control.release(fd)


# --- consumption --------------------------------------------------------------------------------

def blocks_text(items: list[dict[str, Any]]) -> str:
    """One block per message, in queue order, text exactly as sent."""
    total = len(items)
    return "\n".join(
        f"Queued caller follow-up {number} of {total} (message {item['id']}, queued "
        f"{item.get('queued_at')}):\n{item['text']}\n"
        for number, item in enumerate(items, 1)
    )


def compose(items: list[dict[str, Any]], explicit_prompt: str | None) -> str:
    """The prompt for a continuation that consumes `items`: the blocks, a blank line, then the
    explicit prompt verbatim — or `CLOSING_LINE` when there is none."""
    return blocks_text(items) + "\n" + (explicit_prompt if explicit_prompt is not None else CLOSING_LINE)


def _not_started(message: str) -> ValueError:
    error = ValueError(message)
    error.harflow_not_started = True  # type: ignore[attr-defined]
    return error


@task_operation
def _reserve(
    log_dir: Path, task_id: str, successor: str, owner: dict[str, Any] | None
) -> list[dict[str, Any]]:
    """Reserve every queued message for `successor`, sealing the ledger. Blocking."""
    try:
        with _locked(log_dir, task_id):
            record = store.read(log_dir, task_id)
            try:
                doc = _load(log_dir, task_id)
            except Unavailable as exc:
                raise _not_started(
                    f"{exc}. Inspect the follow-up ledger outside the engine before continuing"
                ) from exc
            if _resolve(log_dir, record, doc, _lazy(lambda: successor_candidates(log_dir, record))
                        if record is not None else None):
                _persist(log_dir, task_id, doc)
            queued = sorted((item for item in doc["items"] if item["status"] == QUEUED), key=lambda i: i["seq"])
            if not queued:
                _check_source(state_path(log_dir, task_id), doc)
                return []
            now = _now()
            for item in queued:
                item.update(status=RESERVED, successor_task_id=successor, owner=owner, at=now)
            doc["sealed_by"] = successor
            _INFLIGHT.add(_key(log_dir, successor))
            try:
                _store(log_dir, task_id, doc)
                return [dict(item) for item in queued]
            except BaseException:
                _INFLIGHT.discard(_key(log_dir, successor))
                raise
    except control.LockTimeout:
        raise _not_started(
            f"task {task_id}'s follow-up queue is locked by another process; try again shortly"
        ) from None
    except (Unavailable, _TooLarge, OSError) as exc:
        raise _not_started(f"could not reserve the queued follow-ups of task {task_id}: {exc}") from exc


@task_operation
def finish(
    log_dir: Path, task_id: str, successor: str, ids: list[str], outcome: str, reason: str | None = None
) -> bool:
    """Write an attempt's terminal state (`consumed`, `released` or `delivery_unknown`). Blocking.
    Ownership failures raise; ordinary failed writes are remembered and retried at the next lock acquisition,
    and false is returned."""
    try:
        with _locked(log_dir, task_id):
            doc = _load(log_dir, task_id)
            _apply_terminal(doc, successor, ids, outcome, reason)
            _store(log_dir, task_id, doc)
        return True
    except (control.LockTimeout, Unavailable, _TooLarge, OSError, store.TaskDocumentError):
        log.warning("task %s: follow-up %s write failed; will retry", task_id, outcome, exc_info=True)
        with _PENDING_GUARD:
            _PENDING.setdefault(_key(log_dir, task_id), []).append((successor, ids, outcome, reason))
        return False
    finally:
        _INFLIGHT.discard(_key(log_dir, successor))


@task_operation
def handoff(log_dir: Path, task_id: str, successor: str, owner: dict[str, Any] | None) -> None:
    """Re-own this successor's `reserved` items by its launch owner. Blocking."""
    try:
        with _locked(log_dir, task_id):
            doc = _load(log_dir, task_id)
            changed = False
            for item in doc["items"]:
                if item["status"] == RESERVED and item.get("successor_task_id") == successor and owner is not None:
                    item["owner"] = owner
                    changed = True
            if changed:
                _store(log_dir, task_id, doc)
    except (control.LockTimeout, Unavailable, _TooLarge, OSError, store.TaskDocumentError):
        log.warning("task %s: follow-up hand-off write failed; the launch receipt still decides", task_id, exc_info=True)
    finally:
        # This process no longer decides the item; the receipt and its owner do.
        _INFLIGHT.discard(_key(log_dir, successor))


@dataclass
class Attempt:
    """One consumption of a task's queued messages by one successor."""

    log_dir: Path
    task_id: str
    successor_id: str
    items: list[dict[str, Any]] = field(default_factory=list)

    @property
    def state_layout(self):
        return task_layout(self.log_dir)

    @property
    def ids(self) -> list[str]:
        return [item["id"] for item in self.items]

    def compose(self, explicit_prompt: str | None) -> str:
        return compose(self.items, explicit_prompt)

    @layout_operation
    async def _terminal(self, outcome: str, reason: str | None = None) -> bool:
        work = owned_job(self.state_layout, run_owned_worker(self.state_layout,
            finish, self.log_dir, self.task_id, self.successor_id, self.ids, outcome, reason))
        try:
            return await asyncio.shield(work)
        except asyncio.CancelledError:
            try:
                await work
            except StateOwnershipError:
                raise
            except Exception:
                pass
            raise

    @layout_operation
    async def consumed(self, registry: Any, task: Any) -> None:
        """The successor started: record the consumption. A write that fails is a notice on the
        successor, never an error."""
        if not await self._terminal(CONSUMED):
            self._notice(registry, task)

    @layout_operation
    async def handed_off(self, launch: Any) -> None:
        """The successor was acknowledged while it waits for resources: nothing is consumed yet.

        The owner finalizes consumption at spawn and releases the messages if the launch never
        starts. Until then the reservation belongs to the launch owner, so a caller that dies now
        neither strands it nor resolves it as `delivery_unknown`.
        """
        work = owned_job(self.state_layout, run_owned_worker(self.state_layout, handoff, self.log_dir, self.task_id,
                                                              self.successor_id, getattr(launch, "receipt", {}).get("owner")))
        await asyncio.shield(work)

    @layout_operation
    async def fail(self, registry: Any, error: BaseException, spawn_called: bool) -> None:
        """The launch failed. Provably not started releases the messages, a successor that exists
        consumes them, and anything else leaves them `delivery_unknown`."""
        if getattr(error, "harflow_not_started", False) is True or not spawn_called:
            await self._terminal("released")
        elif registry.get(self.successor_id) is not None or store.record_path(self.log_dir, self.successor_id).exists():
            await self._terminal(CONSUMED)
        else:
            await self._terminal(DELIVERY_UNKNOWN, (
                f"the launch of {self.successor_id} failed ({type(error).__name__}) after the queued "
                "messages were handed to it; they may have reached an agent, so inspect before "
                "resending"))

    @layout_operation
    def _notice(self, registry: Any, task: Any) -> None:
        from .task_attachment import AttachedTask
        if isinstance(task, AttachedTask):
            task._add_observer_notice(
                f'Queued follow-ups from {self.task_id} were handed to this successor, but the source '
                'ledger consumption update is pending reconciliation. This observer did not change owner metadata.')
            return
        try:
            task.bridge_notices.append(
                f"The queued follow-ups of {self.task_id} were delivered to this task, but marking "
                "them consumed failed; they read as consumed once the ledger is next repaired."
            )
            registry.persist(task)
        except StateOwnershipError:
            raise
        except Exception:
            log.debug("could not record the follow-up notice", exc_info=True)


@task_operation
async def begin(
    log_dir: Path, task_id: str, successor_id: str, owner: dict[str, Any] | None
) -> Attempt:
    """Reserve the queued messages of `task_id` for `successor_id`. An attempt with no items means
    nothing was queued (a lock round trip, no write). Raises a `ValueError` flagged
    `harflow_not_started` when the ledger cannot be reserved."""
    from .state_layout import require_dispatch_layout
    require_dispatch_layout(log_dir)
    attempt = Attempt(log_dir, task_id, successor_id)
    # The lock is taken even with no ledger yet, so a send during the reservation is ordered by it.
    # A send landing right after an empty begin() is accepted, and later resolves to `undeliverable`
    # (superseded by this continuation) with its text kept.
    work = owned_job(task_layout(log_dir), run_owned_worker(task_layout(log_dir), _reserve, log_dir, task_id, successor_id, owner))
    try:
        attempt.items = await asyncio.shield(work)
    except asyncio.CancelledError:
        # The worker cannot be interrupted: let it settle, then undo a reservation it made.
        try:
            reserved = await work
            if reserved:
                await asyncio.shield(owned_job(task_layout(log_dir), run_owned_worker(task_layout(log_dir),
                    finish, log_dir, task_id, successor_id, [item["id"] for item in reserved], "released")))
        except StateOwnershipError:
            raise
        except Exception:
            pass
        raise
    return attempt
