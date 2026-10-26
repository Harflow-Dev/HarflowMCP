"""Persisted resource reservations: named, exclusive, cross-process locks for dispatched work.

A *resource* is just a validated name (`ios-simulator`, `repo:main`). A *reservation* is one
request for one or more names. The ledger is a private JSON document,
`<state_root>/resources/ledger.json` (format `harflow.resource-ledger`, v1), so a reservation outlives
the process that made it and every server and `harflow-ctl` process on the machine sees the same
queue. Nothing here knows what a task or a workflow is: an entry carries an *owner* (kind plus ids)
and the owner-specific "is it finished?" question is answered by a resolver the caller registers
(`reap`). Stage A ships no resolvers, so every entry is kept until its owner releases it or a person
releases it with `harflow-ctl resources-release`.

**The lock is a leaf.** One `flock(<state_root>/resources/.ledger.lock)` guards each read-modify-
write of the ledger. It is held for exactly one bounded read, edit and atomic replace; never across an
`await`, a subprocess (including the `ps` behind identity checks), Git, or the acquisition of any
other lock. Taking it while a session/chain/store lock is held is allowed; the reverse never
happens. It is taken with a non-blocking retry loop and a timeout, so a stopped process holding it
surfaces as `ResourceLedgerUnavailable` instead of freezing an event loop. Every change to an entry
bumps its `generation`, which is what lets the reaper apply a verdict it computed outside the lock
only if nothing moved in the meantime.

**Grant rule.** A queued entry is granted all of its names at once, or none. It is granted only if
every name has a free slot (`capacity(name)`, 1 for every name today; the hook is where configured
capacity will plug in) *and* no earlier entry that shares a name is still queued. That is strict
per-name FIFO: a large request cannot be starved by a stream of small ones, and an entry never holds
part of what it asked for while it waits, so two waiters cannot each hold what the other needs.

**Root declaration (no waiting while holding).** A requester that already runs under a holding scope
may ask only for names that scope declared; those are granted at once as a *borrow* (no capacity
consumed, no queueing). Any other name is refused with `resource_not_declared_by_scope`: declare
every resource at the outermost scope. A scope that holds nothing named (a workflow attempt with
no resources of its own still holds a checkout lease) refuses every name. So a waiter never owns
anything another waiter could need, and no wait-for cycle can form. The caller resolves the
scope (lineage, workflow receipts) and passes it in as a `Scope`; this module does not detect it.

**Release and reaping.** The owner releases on its normal settlement path. A reservation with
unresolved borrowers is kept, flagged `release_requested`, and goes when its last borrower does. The
reaper (`reap`) covers everything else: it reads under the lock, decides each entry's owner state
*outside* the lock through the resolver for the entry's owner kind, then re-takes the lock and
removes only entries whose generation is unchanged and whose verdict was `settled` or
`never_started`. Anything else, including a resolver that raises and a kind with no resolver, keeps
the entry: uncertainty never frees a resource.

**Corruption is never silently reset.** A ledger that cannot be parsed or qualified makes every
operation raise `ResourceLedgerCorrupt`. `repair` is the one explicit way out: it moves the file
aside to `ledger.corrupt-<timestamp>.json` and starts an empty ledger. Later stages extend it to
rebuild holders from durable launch and run state; in stage A there is nothing to rebuild from.
"""

from __future__ import annotations

import asyncio
import copy
import errno
import fcntl
import inspect
import json
import os
import re
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .state_compatibility import persisted_compatible
from .wire_contracts import PRODUCT, PRODUCT_VERSION

LEDGER_FORMAT = "harflow.resource-ledger"
RESOURCES_DIRECTORY = "resources"
LEDGER_NAME = "ledger.json"
LOCK_NAME = ".ledger.lock"

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,63}$")
RESERVATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
MAX_NAMES = 16
MAX_ENTRIES = 4096

OWNER_KINDS = ("task", "workflow_run", "workflow_attempt")
_OWNER_ID_FIELDS = ("task_id", "workflow_run_id", "activation_id")
STATES = ("queued", "held")

# Resolver verdicts. Only the first two free an entry.
SETTLED = "settled"
NEVER_STARTED = "never_started"
KEEP = "keep"
_REMOVING_VERDICTS = frozenset({SETTLED, NEVER_STARTED})

POLL_INITIAL_SECONDS = 0.5
POLL_MAX_SECONDS = 2.0
LOCK_TIMEOUT_SECONDS = 10.0


class ResourceError(ValueError):
    """Base of every typed refusal; `code` is the stable machine-readable reason. A ValueError so a
    request refused before launch reads as invalid parameters everywhere one already does."""

    code = "resource_error"


class InvalidResourceRequest(ResourceError):
    code = "invalid_resource_request"


class ResourceNotDeclaredByScope(ResourceError):
    code = "resource_not_declared_by_scope"


class ResourceWaitExpired(ResourceError):
    code = "resource_wait_expired"


class ResourceWaitCancelled(ResourceError):
    code = "resource_wait_cancelled"


class UnknownReservation(ResourceError):
    code = "unknown_reservation"


class ResourceLedgerCorrupt(ResourceError):
    code = "resource_ledger_corrupt"


class ResourceLedgerUnavailable(ResourceError):
    code = "resource_ledger_unavailable"


class LedgerNotCorrupt(ResourceError):
    code = "ledger_not_corrupt"


def capacity(name: str) -> int:
    """Slots for `name`. Capacity 1 for every name; the seam where configured capacity plugs in."""
    return 1


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Scope:
    """The holding scope a requester runs under, as resolved by the caller.

    `Scope(reservation_id="X")`: borrow from reservation X. `Scope(None)`: a holding scope that
    reserved nothing named (for example a workflow attempt without resources), which refuses every
    name. The absence of any scope is passed as `scope=None`, and queues normally.
    """

    reservation_id: str | None


@dataclass(frozen=True)
class Owner:
    """Who owns an entry, by id only; liveness is never stored."""

    kind: str
    task_id: str | None = None
    workflow_run_id: str | None = None
    activation_id: str | None = None

    def as_dict(self) -> dict[str, str]:
        document = {"kind": self.kind}
        for key in _OWNER_ID_FIELDS:
            value = getattr(self, key)
            if value is not None:
                document[key] = value
        return document


@dataclass(frozen=True)
class Entry:
    """Read-only view of one ledger entry."""

    reservation_id: str
    names: tuple[str, ...]
    state: str
    owner: Owner
    enqueued_at: str
    generation: int
    borrows_from: str | None = None
    granted_at: str | None = None
    deadline_at: str | None = None
    release_requested: bool = False

    @property
    def held(self) -> bool:
        return self.state == "held"


@dataclass
class ReapReport:
    removed: list[str] = field(default_factory=list)
    deferred: list[str] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


Resolver = Callable[[Entry], str]


def validate_names(names: Iterable[str]) -> tuple[str, ...]:
    """Sorted, unique names, or `InvalidResourceRequest`. Duplicates are refused, never merged."""
    if isinstance(names, (str, bytes)):
        raise InvalidResourceRequest("resource names must be a list of names, not a single string")
    listed = list(names)
    if not 1 <= len(listed) <= MAX_NAMES:
        raise InvalidResourceRequest(f"request 1 to {MAX_NAMES} resource names, got {len(listed)}")
    for name in listed:
        if type(name) is not str or NAME_RE.fullmatch(name) is None:
            raise InvalidResourceRequest(
                f"invalid resource name {name!r}: use lowercase letters, digits and . _ : - "
                "(1 to 64 characters, starting with a letter or digit)"
            )
    if len(set(listed)) != len(listed):
        raise InvalidResourceRequest("duplicate resource names in one request")
    return tuple(sorted(listed))


def validate_reservation_id(value: Any) -> str:
    if type(value) is not str or RESERVATION_ID_RE.fullmatch(value) is None:
        raise InvalidResourceRequest(f"invalid reservation id: {value!r}")
    return value


def _owner_from(value: Owner | Mapping[str, Any]) -> Owner:
    try:
        document = value.as_dict() if isinstance(value, Owner) else dict(value)
        kind = document.get("kind")
        if kind not in OWNER_KINDS:
            raise ValueError(f"owner kind must be one of {', '.join(OWNER_KINDS)}")
        extra = set(document) - {"kind", *_OWNER_ID_FIELDS}
        if extra:
            raise ValueError(f"unknown owner fields: {sorted(extra)}")
        ids = {}
        for key in _OWNER_ID_FIELDS:
            item = document.get(key)
            if item is not None and (type(item) is not str or not item or len(item) > 256):
                raise ValueError(f"owner {key} must be a nonempty string")
            ids[key] = item
        return Owner(kind, **ids)
    except (TypeError, ValueError) as exc:
        raise InvalidResourceRequest(f"invalid owner: {exc}") from None


def _entry_view(raw: Mapping[str, Any]) -> Entry:
    owner = raw["owner"]
    return Entry(
        reservation_id=raw["reservation_id"],
        names=tuple(raw["names"]),
        state=raw["state"],
        owner=Owner(owner["kind"], **{key: owner.get(key) for key in _OWNER_ID_FIELDS}),
        enqueued_at=raw["enqueued_at"],
        generation=raw["generation"],
        borrows_from=raw.get("borrows_from"),
        granted_at=raw.get("granted_at"),
        deadline_at=raw.get("deadline_at"),
        release_requested=bool(raw.get("release_requested", False)),
    )


def _validate_document(document: Any) -> list[dict[str, Any]]:
    """Return the entry list of a qualified ledger, or raise ValueError describing the defect."""
    if type(document) is not dict:
        raise ValueError("the ledger is not a JSON object")
    if document.get("product") != PRODUCT or document.get("format") != LEDGER_FORMAT:
        raise ValueError("missing or foreign ledger product/format")
    if type(document.get("v")) is not int or document["v"] != 1:
        raise ValueError("unsupported ledger version")
    if not persisted_compatible(document.get("product_version"), LEDGER_FORMAT):
        raise ValueError("incompatible ledger product line")
    entries = document.get("entries")
    if type(entries) is not list or len(entries) > MAX_ENTRIES:
        raise ValueError("ledger entries must be a bounded list")
    seen: set[str] = set()
    for raw in entries:
        if type(raw) is not dict:
            raise ValueError("a ledger entry is not an object")
        rid = raw.get("reservation_id")
        if type(rid) is not str or RESERVATION_ID_RE.fullmatch(rid) is None or rid in seen:
            raise ValueError("a ledger entry has a missing, invalid or duplicate reservation_id")
        seen.add(rid)
        names = raw.get("names")
        if type(names) is not list or validate_names(names) != tuple(names):
            raise ValueError(f"entry {rid} has invalid or unsorted names")
        if raw.get("state") not in STATES:
            raise ValueError(f"entry {rid} has an invalid state")
        owner = raw.get("owner")
        if type(owner) is not dict:
            raise ValueError(f"entry {rid} has no owner")
        _owner_from(owner)
        if type(raw.get("generation")) is not int or raw["generation"] < 1:
            raise ValueError(f"entry {rid} has an invalid generation")
        if type(raw.get("enqueued_at")) is not str:
            raise ValueError(f"entry {rid} has no enqueued_at")
        for key in ("granted_at", "deadline_at", "borrows_from"):
            if raw.get(key) is not None and type(raw[key]) is not str:
                raise ValueError(f"entry {rid} has an invalid {key}")
        if raw["state"] == "held" and raw.get("granted_at") is None:
            raise ValueError(f"held entry {rid} has no granted_at")
        if raw.get("borrows_from") is not None and raw["state"] != "held":
            raise ValueError(f"entry {rid} borrows without being held")
        if "release_requested" in raw and type(raw["release_requested"]) is not bool:
            raise ValueError(f"entry {rid} has an invalid release_requested")
    return entries


def _empty_document() -> dict[str, Any]:
    return {"product": PRODUCT, "format": LEDGER_FORMAT, "v": 1, "product_version": PRODUCT_VERSION, "entries": []}


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, document: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x") as handle:
            os.fchmod(handle.fileno(), 0o600)
            json.dump(document, handle, ensure_ascii=False, allow_nan=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


class _Transaction:
    """The decoded ledger inside one lock hold. `dirty` is set by whoever changes an entry."""

    def __init__(self, entries: list[dict[str, Any]], capacity_of: Callable[[str], int], clock: Callable[[], str]) -> None:
        self.entries = entries
        self.capacity_of = capacity_of
        self.clock = clock
        self.dirty = False

    def find(self, reservation_id: str) -> dict[str, Any] | None:
        return next((raw for raw in self.entries if raw["reservation_id"] == reservation_id), None)

    def touch(self, raw: dict[str, Any]) -> None:
        raw["generation"] += 1
        self.dirty = True

    def borrowers_of(self, reservation_id: str) -> list[dict[str, Any]]:
        return [raw for raw in self.entries if raw.get("borrows_from") == reservation_id]

    def grant_pass(self) -> None:
        """Strict per-name FIFO, all or nothing. Idempotent: a pass that grants nothing writes nothing."""
        held_count: dict[str, int] = {}
        for raw in self.entries:
            if raw["state"] == "held" and raw.get("borrows_from") is None:
                for name in raw["names"]:
                    held_count[name] = held_count.get(name, 0) + 1
        blocked: set[str] = set()
        for raw in self.entries:
            if raw["state"] != "queued":
                continue
            names = raw["names"]
            free = all(held_count.get(name, 0) < self.capacity_of(name) for name in names)
            if free and blocked.isdisjoint(names):
                raw["state"] = "held"
                raw["granted_at"] = self.clock()
                self.touch(raw)
                for name in names:
                    held_count[name] = held_count.get(name, 0) + 1
            else:
                blocked.update(names)

    def release(self, reservation_id: str) -> str:
        """`released`, `release_requested` (borrowers unresolved) or `absent`."""
        raw = self.find(reservation_id)
        if raw is None:
            return "absent"
        if self.borrowers_of(reservation_id):
            if not raw.get("release_requested"):
                raw["release_requested"] = True
                self.touch(raw)
            return "release_requested"
        self.entries.remove(raw)
        self.dirty = True
        parent_id = raw.get("borrows_from")
        if parent_id is not None:
            parent = self.find(parent_id)
            if parent is not None and parent.get("release_requested") and not self.borrowers_of(parent_id):
                self.entries.remove(parent)
        return "released"


class ResourceLedger:
    """The ledger of one state root. Cheap to construct; holds no file open between calls."""

    def __init__(
        self,
        state_root: str | os.PathLike[str],
        *,
        capacity_of: Callable[[str], int] = capacity,
        clock: Callable[[], str] = _now,
        lock_timeout: float = LOCK_TIMEOUT_SECONDS,
    ) -> None:
        self.directory = Path(state_root) / RESOURCES_DIRECTORY
        self.path = self.directory / LEDGER_NAME
        self._lock_path = self.directory / LOCK_NAME
        self._capacity_of = capacity_of
        self._clock = clock
        self._lock_timeout = lock_timeout

    # ---- lock and storage ------------------------------------------------------------------

    def _lock(self) -> int:
        """Take the leaf flock, retrying without blocking, and return its descriptor."""
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(self._lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        deadline = time.monotonic() + self._lock_timeout
        delay = 0.001
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return descriptor
            except OSError as exc:
                if exc.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                    os.close(descriptor)
                    raise
                if time.monotonic() >= deadline:
                    os.close(descriptor)
                    raise ResourceLedgerUnavailable("the resource ledger lock is held by another process") from None
                time.sleep(delay)
                delay = min(delay * 2, 0.05)

    @staticmethod
    def _unlock(descriptor: int) -> None:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _read_entries(self) -> list[dict[str, Any]]:
        try:
            with self.path.open("rb") as handle:
                payload = handle.read(64 * 1024 * 1024)
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise ResourceLedgerUnavailable(f"the resource ledger cannot be read: {exc}") from None
        try:
            return _validate_document(json.loads(payload))
        except (ValueError, RecursionError, UnicodeDecodeError, InvalidResourceRequest) as exc:
            raise ResourceLedgerCorrupt(
                f"the resource ledger at {self.path} is corrupt ({exc}); every resource operation refuses "
                "until a person runs `harflow-ctl resources-repair`"
            ) from None

    def _transact(self, action: Callable[[_Transaction], Any]) -> Any:
        """One bounded read-modify-write under the leaf lock. Nothing in `action` may block or await."""
        descriptor = self._lock()
        try:
            transaction = _Transaction(self._read_entries(), self._capacity_of, self._clock)
            result = action(transaction)
            if transaction.dirty:
                document = _empty_document()
                document["entries"] = transaction.entries
                _atomic_write(self.path, document)
            return result
        finally:
            self._unlock(descriptor)

    def _read_only(self) -> list[dict[str, Any]]:
        """A consistent read that never creates the directory, for pure readers."""
        if not self.directory.exists():
            return []
        descriptor = self._lock()
        try:
            return copy.deepcopy(self._read_entries())
        finally:
            self._unlock(descriptor)

    # ---- requests --------------------------------------------------------------------------

    def request(
        self,
        names: Iterable[str],
        owner: Owner | Mapping[str, Any],
        *,
        scope: Scope | None = None,
        deadline_at: str | None = None,
        reservation_id: str | None = None,
    ) -> Entry:
        """Enqueue (or borrow) and run the grant pass in one transaction.

        The result is `held` when granted or borrowed, else `queued`. `reservation_id` lets an async
        caller know the id before the transaction runs, so a cancellation can still release it.
        """
        wanted = validate_names(names)
        who = _owner_from(owner)
        rid = validate_reservation_id(reservation_id) if reservation_id is not None else uuid.uuid4().hex

        def action(transaction: _Transaction) -> Entry:
            if transaction.find(rid) is not None:
                raise InvalidResourceRequest(f"reservation {rid} already exists")
            if len(transaction.entries) >= MAX_ENTRIES:
                raise InvalidResourceRequest("the resource ledger is full")
            now = transaction.clock()
            raw: dict[str, Any] = {
                "reservation_id": rid,
                "names": list(wanted),
                "state": "queued",
                "owner": who.as_dict(),
                "enqueued_at": now,
                "generation": 1,
            }
            if deadline_at is not None:
                raw["deadline_at"] = deadline_at
            if scope is not None:
                parent = self._borrow_root(transaction, scope, wanted)
                raw.update(state="held", granted_at=now, borrows_from=parent["reservation_id"])
            transaction.entries.append(raw)
            transaction.dirty = True
            transaction.grant_pass()
            return _entry_view(raw)

        return self._transact(action)

    @staticmethod
    def _borrow_root(transaction: _Transaction, scope: Scope, wanted: tuple[str, ...]) -> dict[str, Any]:
        """The held entry the borrow attaches to, or `ResourceNotDeclaredByScope` (fail closed)."""
        advice = "declare every resource at the outermost scope that holds it"
        if scope.reservation_id is None:
            raise ResourceNotDeclaredByScope(
                f"this scope declared no resources, so it cannot take {', '.join(wanted)}; {advice}"
            )
        holder = transaction.find(scope.reservation_id)
        if holder is None or holder["state"] != "held" or holder.get("release_requested"):
            raise ResourceNotDeclaredByScope(
                f"the enclosing scope's reservation {scope.reservation_id} is not currently held, so it "
                f"cannot lend {', '.join(wanted)}; {advice}"
            )
        missing = [name for name in wanted if name not in holder["names"]]
        if missing:
            raise ResourceNotDeclaredByScope(
                f"the enclosing scope did not declare {', '.join(missing)}; {advice}"
            )
        # A nested borrow attaches to the root, so the root's release waits for every descendant.
        root_id = holder.get("borrows_from")
        if root_id is None:
            return holder
        root = transaction.find(root_id)
        return root if root is not None else holder

    def check_borrow(self, names: Iterable[str], scope: Scope) -> None:
        """Raise `ResourceNotDeclaredByScope` unless `scope` would lend every name right now.

        A read-only early answer for a caller that should be refused before anything is launched;
        the owner repeats the real check in `request`, because the scope can change in between.
        """
        wanted = validate_names(names)
        transaction = _Transaction(self._read_only(), self._capacity_of, self._clock)
        self._borrow_root(transaction, scope, wanted)

    def restore_held(self, names: Iterable[str], owner: Owner | Mapping[str, Any], reservation_id: str) -> Entry | None:
        """Re-create a held entry from durable evidence (`resources-repair`). Never queues, never
        borrows, and never displaces a current holder: `None` when a name is already held."""
        wanted = validate_names(names)
        who = _owner_from(owner)
        rid = validate_reservation_id(reservation_id)

        def action(transaction: _Transaction) -> Entry | None:
            if transaction.find(rid) is not None:
                return _entry_view(transaction.find(rid))
            for raw in transaction.entries:
                if raw["state"] == "held" and raw.get("borrows_from") is None and not set(raw["names"]).isdisjoint(wanted):
                    return None
            now = transaction.clock()
            raw = {"reservation_id": rid, "names": list(wanted), "state": "held", "owner": who.as_dict(),
                   "enqueued_at": now, "granted_at": now, "generation": 1}
            transaction.entries.append(raw)
            transaction.dirty = True
            return _entry_view(raw)

        return self._transact(action)

    def poll(self, reservation_id: str, *, dequeue_if_queued: bool = False) -> Entry:
        """Run the grant pass and report the entry. Writes nothing when nothing changed.

        `dequeue_if_queued` removes the entry in the same transaction when it is still queued, so a
        wait that expires or is cancelled cannot lose a grant that landed at the last moment: if the
        entry turned out `held`, it is returned held and the caller decides what to do with it.
        """

        def action(transaction: _Transaction) -> Entry:
            transaction.grant_pass()
            raw = transaction.find(reservation_id)
            if raw is None:
                raise UnknownReservation(f"unknown reservation {reservation_id}")
            view = _entry_view(raw)
            if dequeue_if_queued and raw["state"] == "queued":
                transaction.entries.remove(raw)
                transaction.dirty = True
            return view

        return self._transact(action)

    def release(self, reservation_id: str) -> str:
        """Release (or dequeue) an entry: `released`, `release_requested` or `absent` (idempotent)."""
        validate_reservation_id(reservation_id)

        def action(transaction: _Transaction) -> str:
            outcome = transaction.release(reservation_id)
            transaction.grant_pass()
            return outcome

        return self._transact(action)

    def get(self, reservation_id: str) -> Entry | None:
        for raw in self._read_only():
            if raw["reservation_id"] == reservation_id:
                return _entry_view(raw)
        return None

    def entries(self) -> list[Entry]:
        return [_entry_view(raw) for raw in self._read_only()]

    async def acquire(
        self,
        names: Iterable[str],
        owner: Owner | Mapping[str, Any],
        *,
        scope: Scope | None = None,
        timeout: float | None = None,
        should_continue: Callable[[], Any] | None = None,
        poll_initial: float = POLL_INITIAL_SECONDS,
        poll_max: float = POLL_MAX_SECONDS,
        reservation_id: str | None = None,
    ) -> Entry:
        """Wait for a grant. The lock is never held across the sleeps between polls.

        Raises `ResourceWaitExpired` at `timeout` seconds and `ResourceWaitCancelled` when
        `should_continue()` (sync or async) says no; either way the queued entry is removed in the
        same transaction as the final poll. A cancellation of this coroutine releases the entry
        too. On success the caller owns the returned reservation and must `release` it.
        `reservation_id` lets a caller record the id durably before the entry can exist.
        """
        rid = validate_reservation_id(reservation_id) if reservation_id is not None else uuid.uuid4().hex
        started = time.monotonic()
        deadline_at = None
        if timeout is not None:
            deadline_at = datetime.fromtimestamp(time.time() + max(timeout, 0), timezone.utc).isoformat()
        try:
            # Synchronous on purpose: one bounded read-modify-write, and a request that ran on a
            # worker thread could still land after a cancellation had already tried to release it.
            entry = self.request(names, owner, scope=scope, deadline_at=deadline_at, reservation_id=rid)
            delay = poll_initial
            while not entry.held:
                reason = None
                if should_continue is not None:
                    verdict = should_continue()
                    if inspect.isawaitable(verdict):
                        verdict = await verdict
                    if not verdict:
                        reason = "cancelled"
                if reason is None and timeout is not None and time.monotonic() - started >= timeout:
                    reason = "expired"
                entry = await asyncio.to_thread(self.poll, rid, dequeue_if_queued=reason is not None)
                if entry.held:
                    break
                if reason == "cancelled":
                    raise ResourceWaitCancelled(f"the wait for {', '.join(entry.names)} was cancelled")
                if reason == "expired":
                    raise ResourceWaitExpired(
                        f"{', '.join(entry.names)} not available" + (" (a wait of 0 grants now or fails)" if timeout == 0 else f" after waiting {timeout:g}s"))
                await asyncio.sleep(delay)
                delay = min(delay * 2, poll_max)
            return entry
        except BaseException:
            # Failure, expiry or cancellation: whatever is ours must not outlive this call. Sync on
            # purpose: an await in a cancelled coroutine would be cancelled again.
            try:
                self.release(rid)
            except ResourceError:
                pass
            raise

    # ---- reading ---------------------------------------------------------------------------

    def snapshot(self) -> list[dict[str, Any]]:
        """Per-name view for `harflow-ctl resources --json`, sorted by name."""
        entries = self._read_only()
        names = sorted({name for raw in entries for name in raw["names"]})
        resources = []
        for name in names:
            holders, queue = [], []
            for raw in entries:
                if name not in raw["names"]:
                    continue
                row: dict[str, Any] = {
                    "reservation_id": raw["reservation_id"],
                    "owner": raw["owner"],
                    "enqueued_at": raw["enqueued_at"],
                    "generation": raw["generation"],
                }
                if raw["state"] == "held":
                    row["granted_at"] = raw["granted_at"]
                    if raw.get("borrows_from") is not None:
                        row["borrows_from"] = raw["borrows_from"]
                    if raw.get("release_requested"):
                        row["release_requested"] = True
                    holders.append(row)
                else:
                    if raw.get("deadline_at") is not None:
                        row["deadline_at"] = raw["deadline_at"]
                    queue.append(row)
            resources.append({"name": name, "capacity": self._capacity_of(name), "holders": holders, "queue": queue})
        return resources

    # ---- reaping ---------------------------------------------------------------------------

    def reap(self, resolvers: Mapping[str, Resolver]) -> ReapReport:
        """Free entries whose owner is provably finished or never started; keep everything else.

        Three steps, so no resolver (which may run `ps`) ever runs under the lock: read, resolve,
        then re-take the lock and apply each removal only if the entry's generation is unchanged.
        """
        report = ReapReport()
        observed = self.entries()
        verdicts: dict[str, tuple[int, str]] = {}
        for entry in observed:
            resolver = resolvers.get(entry.owner.kind)
            if resolver is None:
                report.kept.append(entry.reservation_id)
                continue
            try:
                verdict = resolver(entry)
            except Exception as exc:  # a resolver that cannot decide keeps the entry
                report.kept.append(entry.reservation_id)
                report.errors[entry.reservation_id] = f"{type(exc).__name__}: {exc}"
                continue
            if verdict in _REMOVING_VERDICTS:
                verdicts[entry.reservation_id] = (entry.generation, verdict)
            else:
                report.kept.append(entry.reservation_id)
        if not verdicts:
            return report

        def action(transaction: _Transaction) -> None:
            for rid, (generation, _verdict) in verdicts.items():
                raw = transaction.find(rid)
                if raw is None or raw["generation"] != generation:
                    report.stale.append(rid)
                    continue
                outcome = transaction.release(rid)
                (report.removed if outcome == "released" else report.deferred).append(rid)
            transaction.grant_pass()

        self._transact(action)
        return report

    # ---- repair ----------------------------------------------------------------------------

    def repair(self) -> Path:
        """Move a corrupt ledger aside and start an empty one; return where the old file went.

        Refuses a healthy ledger (`LedgerNotCorrupt`) so the command cannot be used to wipe live
        reservations. Stage A rebuilds nothing: later stages extend this to restore holders from
        durable launch and run state, and until then a person decides what was lost.
        """
        descriptor = self._lock()
        try:
            try:
                self._read_entries()
            except ResourceLedgerCorrupt:
                pass
            else:
                raise LedgerNotCorrupt("the resource ledger is healthy; nothing to repair")
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
            target = self.directory / f"ledger.corrupt-{stamp}.json"
            os.rename(self.path, target)
            _fsync_directory(self.directory)
            _atomic_write(self.path, _empty_document())
            return target
        finally:
            self._unlock(descriptor)
