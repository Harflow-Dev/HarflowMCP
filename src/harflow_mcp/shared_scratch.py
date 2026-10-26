"""Shared chain and group scratch folders: allocation, exposure, listing and guarded cleanup.

Every task already owns a throwaway scratch folder (`scratch.py`). Two further folders outlive a
task and are shared on purpose:

* **chain** (`<state_root>/shared-scratch/chain/<scratch_chain_id>/`): the same folder for a task
  and every resume and continuation descended from it.
* **group** (`<state_root>/shared-scratch/group/<slug>-<hash>/`): the same folder for every task
  started with the same `group`.

Rules enforced here rather than hoped for:

* **Keys cannot collide or traverse.** A chain key is a validated task id. A group key is a
  sanitised slug plus 16 hex digits of the SHA-256 of the exact group string, so distinct groups get
  distinct folders and no group text ever becomes a path component.
* **Bridge-owned metadata lives outside the agent-writable folder** (`.meta/<kind>/<key>.json`): the
  exact group string or chain id, `created_at` and `last_used_at`. Reopening a folder verifies the
  recorded identity and fails closed on a mismatch.
* **Nothing is removed while anything may still use it.** Cleanup (`harflow-ctl scratch clean` and
  the retention-driven age policy) refuses while any referencing task, launch or continuation is
  unsettled or uncertain, any referencing record is pinned, or a retained record holds a registered
  artifact under the folder.
* **Allocation, metadata updates and cleanup are serialized** by a per-folder `flock`
  (`.locks/<kind>-<key>.lock`) held only for a bounded check-and-act (allocation holds it from before
  the existence check until the task record is durable). Cleaners rename the folder into `.trash`
  under the lock, unlink the lock file while holding it, and delete the folder outside it. Waiters
  verify after `flock` that they hold the file still at the path and retry otherwise. Lock order:
  session or chain lock, then this lock, then `<id>.lock`; callers that already hold `<id>.lock`
  (retention) only ever try this lock for a bounded time.
"""
from __future__ import annotations

import asyncio
import errno
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from . import store
from .state_access import assert_task_owned, task_operation
from .state_ownership import StateOwnershipError

log = logging.getLogger(__name__)

ROOT_NAME = "shared-scratch"
KINDS = ("chain", "group")
LOCK_DIRECTORY = ".locks"
META_DIRECTORY = ".meta"
TRASH_DIRECTORY = ".trash"
META_FORMAT = "harflow.shared-scratch"
SLUG_MAX = 48
DAYS_ENV = "HARFLOW_SHARED_SCRATCH_DAYS"
DEFAULT_DAYS = 14
LOCK_WAIT_SECONDS = 5.0
METADATA_LOCK_SECONDS = 0.5
SETTLED_SEEN_LIMIT = 256
SIZE_ENTRY_LIMIT = 200_000
TRASH_MIN_AGE_SECONDS = 3600

ENV_TASK = "HARFLOW_SCRATCH"
ENV_TASK_ALIAS = "HARFLOW_TASK_SCRATCH"
ENV_CHAIN = "HARFLOW_CHAIN_SCRATCH"
ENV_GROUP = "HARFLOW_GROUP_SCRATCH"
ENV_NAMES = (ENV_TASK, ENV_TASK_ALIAS, ENV_CHAIN, ENV_GROUP)

WRITABLE = "writable"
NO_ADDITIONAL_GRANT = "no_additional_grant"
SANDBOX_READ_ONLY = "sandbox_read_only"


class SharedScratchError(ValueError):
    """A refused or failed shared-scratch operation; `code` is the stable reason."""

    code = "shared_scratch_error"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code is not None:
            self.code = code


# ---- keys and paths ----------------------------------------------------------------------------


def group_slug(group: str) -> str:
    slug = re.sub(r"[^a-z0-9._-]", "-", group.lower())
    slug = re.sub(r"-{2,}", "-", slug).strip("-.")
    return slug[:SLUG_MAX].rstrip("-.") or "group"


def key_for(kind: str, identity: str) -> str:
    if kind == "chain":
        return store.validate_task_id(identity)
    if kind == "group":
        if not isinstance(identity, str) or not identity:
            raise SharedScratchError("a group must be a nonempty string", code="shared_scratch_invalid")
        digest = hashlib.sha256(identity.encode("utf-8", "surrogatepass")).hexdigest()[:16]
        return f"{group_slug(identity)}-{digest}"
    raise SharedScratchError(f"unknown shared scratch kind {kind!r}", code="shared_scratch_invalid")


def root(log_dir: Path) -> Path:
    return Path(log_dir).parent / ROOT_NAME


def directory(log_dir: Path, kind: str, identity: str) -> Path:
    return root(log_dir) / kind / key_for(kind, identity)


def _meta_path(log_dir: Path, kind: str, key: str) -> Path:
    return root(log_dir) / META_DIRECTORY / kind / f"{key}.json"


def _lock_path(log_dir: Path, kind: str, key: str) -> Path:
    return root(log_dir) / LOCK_DIRECTORY / f"{kind}-{key}.lock"


def _private_directory(path: Path) -> None:
    if path.is_symlink():
        raise SharedScratchError(f"{path} must not be a symlink", code="shared_scratch_unsafe")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if not path.is_dir() or path.is_symlink():
        raise SharedScratchError(f"{path} is not a directory", code="shared_scratch_unsafe")
    os.chmod(path, 0o700)


def _ensure_layout(log_dir: Path, kind: str) -> None:
    base = root(log_dir)
    # Parents are created one by one so a substituted symlink anywhere in the chain is refused.
    _private_directory(base)
    for part in (kind, LOCK_DIRECTORY, TRASH_DIRECTORY, META_DIRECTORY):
        _private_directory(base / part)
    _private_directory(base / META_DIRECTORY / kind)


def days_retained(environ: dict[str, str] | None = None) -> int | None:
    """How many days an unused folder is kept, or None when the age policy is disabled."""
    raw = (os.environ if environ is None else environ).get(DAYS_ENV)
    if raw is None:
        return DEFAULT_DAYS
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s=%r is not an integer; the shared scratch age policy is disabled", DAYS_ENV, raw)
        return None
    return value if value > 0 else None


# ---- locks -------------------------------------------------------------------------------------


class Hold:
    """Held per-folder locks, released together and idempotently."""

    def __init__(self) -> None:
        self._descriptors: list[int] = []

    def add(self, descriptor: int) -> None:
        self._descriptors.append(descriptor)

    def release(self) -> None:
        descriptors, self._descriptors = self._descriptors, []
        for descriptor in descriptors:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _open_lock(log_dir: Path, kind: str, key: str) -> int:
    assert_task_owned(log_dir)
    _ensure_layout(log_dir, kind)
    return os.open(_lock_path(log_dir, kind, key), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)


def _busy(kind: str, key: str) -> SharedScratchError:
    return SharedScratchError(
        f"the shared {kind} scratch folder {key} is busy (another launch or cleanup holds its lock); retry shortly",
        code="shared_scratch_busy")


def _try_lock(log_dir: Path, kind: str, key: str) -> int | None:
    """One attempt at the folder's lock: its descriptor, or None while another holder has it.

    A cleaner unlinks the lock file while holding it, so a waiter that wins the `flock` afterwards holds
    a file no longer at the path. That is not the lock: it is dropped and the attempt repeats on the
    file now at the path, so two parties can never believe they hold the same folder's lock."""
    path = _lock_path(log_dir, kind, key)
    while True:
        descriptor = _open_lock(log_dir, kind, key)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            if exc.errno not in (errno.EAGAIN, errno.EACCES, errno.EWOULDBLOCK):
                raise
            return None
        except BaseException:
            os.close(descriptor)
            raise
        try:
            held, current = os.fstat(descriptor), os.stat(path, follow_symlinks=False)
            if (held.st_dev, held.st_ino) == (current.st_dev, current.st_ino):
                return descriptor
        except FileNotFoundError:
            pass
        except BaseException:
            os.close(descriptor)
            raise
        os.close(descriptor)


async def lock_async(log_dir: Path, kind: str, identity: str, hold: Hold, timeout: float = LOCK_WAIT_SECONDS) -> None:
    """Take the folder's lock into `hold`, polling without blocking the event loop. Bounded."""
    key = key_for(kind, identity)
    deadline = time.monotonic() + timeout
    while True:
        descriptor = _try_lock(log_dir, kind, key)
        if descriptor is not None:
            hold.add(descriptor)
            return
        if time.monotonic() >= deadline:
            raise _busy(kind, key)
        await asyncio.sleep(0.02)


@contextmanager
def locked(log_dir: Path, kind: str, identity: str, *, timeout: float = 2.0) -> Iterator[None]:
    key = key_for(kind, identity)
    deadline = time.monotonic() + timeout
    while True:
        descriptor = _try_lock(log_dir, kind, key)
        if descriptor is not None:
            break
        if time.monotonic() >= deadline:
            raise _busy(kind, key)
        time.sleep(0.01)
    try:
        yield
    finally:
        os.close(descriptor)


# ---- metadata ----------------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _repair_hint(kind: str, key: str) -> str:
    target = f"--chain {key}" if kind == "chain" else "--group <the group name>"
    return f"; once its tasks have settled, repair it with `harflow-ctl scratch clean {target}`, which removes the folder with its metadata"


def _read_meta(log_dir: Path, kind: str, key: str) -> dict[str, Any] | None:
    path = _meta_path(log_dir, kind, key)
    try:
        if path.is_symlink() or path.stat().st_size > 65536:
            raise SharedScratchError(f"shared scratch metadata {path} is unsafe" + _repair_hint(kind, key), code="shared_scratch_unsafe")
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise SharedScratchError(f"shared scratch metadata {path} is unreadable: {exc}" + _repair_hint(kind, key), code="shared_scratch_unsafe") from exc
    if (not isinstance(value, dict) or value.get("format") != META_FORMAT or value.get("kind") != kind
            or value.get("key") != key or not isinstance(value.get("identity"), str)):
        raise SharedScratchError(f"shared scratch metadata {path} is not valid" + _repair_hint(kind, key), code="shared_scratch_unsafe")
    return value


def _write_meta(log_dir: Path, kind: str, key: str, meta: dict[str, Any]) -> None:
    """Replace the metadata atomically. Every writer gets its own temporary file: the event loop and the
    retention thread can write in one process, so a pid-derived name would be shared."""
    path = _meta_path(log_dir, kind, key)
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(meta, handle, ensure_ascii=False, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        moment = datetime.fromisoformat(value)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def touch(log_dir: Path, kind: str, identity: str, when: datetime | None = None, *, timeout: float = METADATA_LOCK_SECONDS) -> bool:
    """Advance `last_used_at` (never backwards), serialized with allocation and cleanup by the folder's lock.

    True when the update is durable or there is nothing to update (no metadata for this identity); False
    when it could not be made (lock busy, unreadable metadata, write failure). The lock is only tried for
    `timeout` seconds, so a caller that holds `<id>.lock` cannot deadlock against a launch holding this one.
    Guarded: bookkeeping never changes an outcome, but ownership failures propagate."""
    assert_task_owned(log_dir)
    try:
        key = key_for(kind, identity)
        if not os.path.lexists(_meta_path(log_dir, kind, key)):
            return True  # no folder was ever allocated for this identity; take no lock for nothing
        with locked(log_dir, kind, identity, timeout=timeout):
            meta = _read_meta(log_dir, kind, key)
            if meta is None or meta["identity"] != identity:
                return True
            moment = when or _now()
            current = _parse_time(meta.get("last_used_at"))
            if current is None or current < moment:
                _write_meta(log_dir, kind, key, {**meta, "last_used_at": moment.isoformat()})
            return True
    except StateOwnershipError:
        raise
    except Exception:
        log.debug("could not update shared scratch metadata", exc_info=True)
        return False


def observe_settlement(log_dir: Path, kind: str, identity: str, task_ids: Iterable[str], now: datetime | None = None,
                       *, timeout: float = METADATA_LOCK_SECONDS) -> bool:
    """Durably record that recovery first found these tasks settled, advancing `last_used_at` to now.

    A task whose exit was never observed has no known finish time: its start time says nothing about when
    it last used the folder. The first observation of its settlement is the best available instant, and it
    must be on disk before cleanup is allowed to count age. Each task id is recorded once (`settled_seen`,
    bounded; an id dropped from the bound only ever extends retention once more). True when every id is
    recorded; False when it could not be, in which case the folder must be kept."""
    assert_task_owned(log_dir)
    ids = list(dict.fromkeys(task_ids))
    if not ids:
        return True
    try:
        key = key_for(kind, identity)
        if not os.path.lexists(_meta_path(log_dir, kind, key)):
            return False
        with locked(log_dir, kind, identity, timeout=timeout):
            meta = _read_meta(log_dir, kind, key)
            if meta is None or meta["identity"] != identity:
                return False
            seen = [value for value in meta.get("settled_seen", []) if isinstance(value, str)]
            fresh = [identifier for identifier in ids if identifier not in seen]
            if not fresh:
                return True
            moment = now or _now()
            current = _parse_time(meta.get("last_used_at"))
            _write_meta(log_dir, kind, key, {
                **meta, "last_used_at": (moment if current is None or current < moment else current).isoformat(),
                "settled_seen": (seen + fresh)[-SETTLED_SEEN_LIMIT:]})
            return True
    except StateOwnershipError:
        raise
    except Exception:
        log.debug("could not record shared scratch settlement", exc_info=True)
        return False


# ---- allocation --------------------------------------------------------------------------------


def allocate(log_dir: Path, kind: str, identity: str, *, now: datetime | None = None) -> Path:
    """Create or reopen the folder; see `allocate_folder`."""
    return allocate_folder(log_dir, kind, identity, now=now)[0]


@task_operation
def allocate_folder(log_dir: Path, kind: str, identity: str, *, now: datetime | None = None) -> tuple[Path, bool]:
    """Create or reopen the folder and say whether this call created it. The caller holds its lock;
    metadata is written before `mkdir`.

    An existing folder is reopened only when its metadata names exactly this identity; a mismatch
    (a hash collision, or tampering) refuses. A folder with no metadata is adopted only because its
    name is derived from this identity, and gets fresh metadata. Only a folder with neither metadata
    nor a directory beforehand counts as created.
    """
    key = key_for(kind, identity)
    _ensure_layout(log_dir, kind)
    path = root(log_dir) / kind / key
    instant = now or _now()
    moment = instant.isoformat()
    meta = _read_meta(log_dir, kind, key)
    created = meta is None and not os.path.lexists(path)
    if meta is not None:
        if meta["identity"] != identity:
            raise SharedScratchError(
                f"the shared {kind} scratch folder {key} belongs to a different {kind}; refusing to reuse it",
                code="shared_scratch_mismatch")
        previous = _parse_time(meta.get("last_used_at"))
        # Never backwards: a metadata update that already recorded a later use stands.
        meta = {**meta, "last_used_at": moment if previous is None or previous < instant else meta["last_used_at"]}
    else:
        meta = {"format": META_FORMAT, "v": 1, "kind": kind, "key": key, "identity": identity,
                "created_at": moment, "last_used_at": moment}
    if path.is_symlink() or (os.path.lexists(path) and not path.is_dir()):
        raise SharedScratchError(f"{path} must be a real directory", code="shared_scratch_unsafe")
    _write_meta(log_dir, kind, key, meta)
    path.mkdir(exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path.resolve(), created


@dataclass
class Allocation:
    """What a run was given: paths, the access it has to each, and the folders' identities."""

    task: str | None = None
    chain: str | None = None
    group: str | None = None
    chain_id: str | None = None
    group_name: str | None = None
    access: dict[str, str] = field(default_factory=dict)
    # Folders whose write grant a nested dispatch was refused; reported on bridge notices, not in `as_dict`.
    denied: list[str] = field(default_factory=list)

    @property
    def writable(self) -> list[str]:
        return [path for name, path in (("task", self.task), ("chain", self.chain), ("group", self.group))
                if path and self.access.get(name) == WRITABLE]

    def as_dict(self) -> dict[str, Any]:
        return {"task": self.task, "chain": self.chain, "group": self.group, "chain_id": self.chain_id,
                "group_name": self.group_name, "access": dict(self.access), "writable": self.writable}


_ACCESS_TEXT = {
    WRITABLE: "writable",
    NO_ADDITIONAL_GRANT: "no write grant was added by Harflow; this backend does not confine writes or approved commands may still write",
    SANDBOX_READ_ONLY: "read-only under the sandbox",
}


def notice(allocation: Allocation) -> str | None:
    """The bridge notice for a run, generated from what it actually got (never from the freedom name)."""
    parts = []
    for label, variable, path, text in (
        ("task", ENV_TASK, allocation.task, "this task only"),
        ("chain", ENV_CHAIN, allocation.chain, "shared by this task and every resume and continuation"),
        ("group", ENV_GROUP, allocation.group, f"shared by every task in group {allocation.group_name!r}"),
    ):
        if path:
            parts.append(f"${variable}={path} ({text}; {_ACCESS_TEXT[allocation.access.get(label, NO_ADDITIONAL_GRANT)]})")
    if not parts:
        return None
    return "Scratch folders, preferred over /tmp: " + "; ".join(parts) + ". Shared folders persist until cleaned (`harflow-ctl scratch`)."


# ---- references --------------------------------------------------------------------------------


def effective_chain(record: Any) -> str:
    """The chain a record belongs to; older records fall back to their continuation root, then themselves."""
    return getattr(record, "scratch_chain_id", None) or getattr(record, "chain_root_task_id", None) or record.task_id


def _under(manifest_path: Path, directory_path: Path) -> bool:
    """Whether a registered artifact lies under the folder, comparing canonical paths (the state root may be
    reached through a symlink while artifacts record resolved paths)."""
    try:
        folder = directory_path.resolve()
        with manifest_path.open("rb") as handle:
            data = json.loads(handle.read(4 * 1024 * 1024 + 1))
        items = data.get("items", []) if isinstance(data, dict) else None
        if not isinstance(items, list):
            return True
        for item in items:
            path = item.get("path") if isinstance(item, dict) else None
            if not isinstance(path, str) or Path(path).resolve().is_relative_to(folder):
                return True
        return False
    except FileNotFoundError:
        return False
    except (OSError, ValueError, TypeError, AttributeError, RuntimeError):
        return True  # unreadable counts as present


class State:
    """One consistent read of everything that can reference a shared folder.

    `State(log_dir)` scans every task record. `State.narrowed(snapshot, kind, identity)` re-reads only the
    members of one folder from an earlier snapshot, with the live receipts, continuations and pins: what
    a per-folder recheck under that folder's lock needs without scanning every record again. A task
    started in the folder since the snapshot cannot be missed that way, because starting one rewrites the
    folder's `last_used_at` under the lock the recheck holds."""

    def __init__(self, log_dir: Path, *, narrowed: tuple["State", str, str] | None = None) -> None:
        from . import followups, retention
        from .task_supervision import reservations
        from .workflows import WorkflowStore
        self.log_dir = Path(log_dir)
        if narrowed is None:
            scan = store.scan(self.log_dir)
            self.records = scan.records
            self.refused = sorted(scan.refusals)
        else:
            snapshot, kind, identity = narrowed
            self.records, self.refused = [], list(snapshot.refused)
            for member in snapshot.members(kind, identity):
                try:
                    fresh = store.read(self.log_dir, member.task_id, include_prompt=False)
                except (store.TaskDocumentError, store.InvalidTaskId):
                    self.refused.append(member.task_id)
                    continue
                if fresh is not None:
                    self.records.append(fresh)
        self.statuses = {r.task_id: store.resolve_status(self.log_dir, r, detail=False)[0] for r in self.records}
        self.settled = {r.task_id: retention._conclusively_settled(r, self.statuses[r.task_id]) for r in self.records}
        pinned = set(WorkflowStore(root=self.log_dir.parent).pinned_tasks())
        try:
            pinned.update(followups.protected_task_ids(self.log_dir))
        except Exception:
            log.debug("could not read protected follow-up tasks", exc_info=True)
        self.pinned = pinned
        self.receipts = [r for r in reservations(self.log_dir) if r["state"] not in ("settled", "not_started")]
        self.continuations: list[dict[str, Any]] = []
        launching = {r["task_id"] for r in self.receipts}
        for path in self.log_dir.glob("*.continuation.json"):
            try:
                if path.stat().st_size > 4096:
                    continue
                value = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            task_id = path.name.removesuffix(".continuation.json")
            if isinstance(value, dict) and (value.get("state") == "reserved" or (value.get("state") == "accepted" and task_id in launching)):
                self.continuations.append(value | {"task_id": task_id})
        self.by_id = {r.task_id: r for r in self.records}

    @classmethod
    def narrowed(cls, snapshot: "State", kind: str, identity: str) -> "State":
        return cls(snapshot.log_dir, narrowed=(snapshot, kind, identity))

    @staticmethod
    def _matches(kind: str, identity: str, chain: str | None, group: str | None) -> bool:
        return chain == identity if kind == "chain" else (group is not None and group == identity)

    def members(self, kind: str, identity: str) -> list[Any]:
        return [r for r in self.records if self._matches(kind, identity, effective_chain(r), r.group)]

    def blockers(self, kind: str, identity: str, directory_path: Path | None = None) -> list[dict[str, Any]]:
        """Why the folder must not be removed now; empty means nothing references it unsettled."""
        found: list[dict[str, Any]] = []
        for identifier in self.refused:
            found.append({"task_id": identifier, "kind": "unreadable_record", "status": "unknown"})
        for record in self.members(kind, identity):
            if not self.settled[record.task_id]:
                found.append({"task_id": record.task_id, "kind": "task", "status": self.statuses[record.task_id]})
            elif record.task_id in self.pinned:
                found.append({"task_id": record.task_id, "kind": "pinned", "status": self.statuses[record.task_id]})
            elif directory_path is not None and _under(self.log_dir / f"{record.task_id}.artifacts.json", directory_path):
                found.append({"task_id": record.task_id, "kind": "artifact", "status": self.statuses[record.task_id]})
        seen = {item["task_id"] for item in found}
        for receipt in self.receipts:
            chain = receipt.get("scratch_chain_id") or receipt.get("chain_root_task_id") or receipt["task_id"]
            if self._matches(kind, identity, chain, receipt.get("group")) and receipt["task_id"] not in seen:
                found.append({"task_id": receipt["task_id"], "kind": "launch", "status": receipt["state"]})
                seen.add(receipt["task_id"])
        for reservation in self.continuations:
            root_record = self.by_id.get(reservation.get("chain_root_task_id") or "")
            if root_record is not None and self._matches(kind, identity, effective_chain(root_record), root_record.group):
                if reservation["task_id"] not in seen:
                    found.append({"task_id": reservation["task_id"], "kind": "continuation", "status": reservation.get("state")})
                    seen.add(reservation["task_id"])
        return found

    def unobserved_settled(self, kind: str, identity: str) -> list[Any]:
        """Members proven settled whose exit nobody observed, so their finish time is unknown."""
        return [r for r in self.members(kind, identity) if self.settled[r.task_id] and store.outcome_unobserved(r)]

    def last_activity(self, kind: str, identity: str) -> datetime | None:
        """The newest observed finish among members. A member whose exit was never observed has no known
        finish: its start time and any backstop `finished_at` say nothing about when it last used the
        folder, so it counts only through `observe_settlement`."""
        latest = None
        for record in self.members(kind, identity):
            if store.outcome_unobserved(record):
                continue
            moment = _parse_time(record.finished_at)
            if moment is not None and (latest is None or moment > latest):
                latest = moment
        return latest


# ---- inventory, cleanup and the age policy -----------------------------------------------------


def directory_size(path: Path, limit: int = SIZE_ENTRY_LIMIT) -> tuple[int, bool]:
    """Bytes under `path` (like `du`: symlinks counted as links, never followed) and whether the walk was cut short."""
    total, visited, stack = 0, 0, [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    visited += 1
                    if visited > limit:
                        return total, True
                    try:
                        info = entry.stat(follow_symlinks=False)
                    except OSError:
                        continue
                    total += info.st_size
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(Path(entry.path))
        except OSError:
            continue
    return total, False


def _entries(log_dir: Path) -> list[tuple[str, str, Path]]:
    found = []
    for kind in KINDS:
        base = root(log_dir) / kind
        try:
            names = sorted(os.listdir(base))
        except OSError:
            continue
        for name in names:
            found.append((kind, name, base / name))
    return found


_BLOCKER_GROUPS = {"task": "active", "launch": "active", "continuation": "active", "unreadable_record": "unreadable",
                   "pinned": "pinned", "artifact": "artifact"}


def blocker_counts(blockers: list[dict[str, Any]]) -> dict[str, int]:
    """Distinct blockers by group: `active` (unsettled tasks, launches and continuations), `unreadable`
    (records that cannot be read), `pinned` and `artifact`. Each blocker is one task id counted once."""
    counts = {"active": 0, "unreadable": 0, "pinned": 0, "artifact": 0}
    seen: set[tuple[str, str]] = set()
    for blocker in blockers:
        group = _BLOCKER_GROUPS.get(blocker["kind"], "active")
        if (group, blocker["task_id"]) not in seen:
            seen.add((group, blocker["task_id"]))
            counts[group] += 1
    return counts


@task_operation
def inventory(log_dir: Path) -> list[dict[str, Any]]:
    """Every shared folder with its exact identity, size, last use and unsettled users."""
    state = State(log_dir)
    result = []
    for kind, name, path in _entries(log_dir):
        if path.is_symlink() or not path.is_dir():
            continue
        try:
            meta = _read_meta(log_dir, kind, name)
        except SharedScratchError:
            meta = None
        identity = meta["identity"] if meta else None
        size, truncated = directory_size(path)
        used = _parse_time(meta.get("last_used_at")) if meta else None
        activity = state.last_activity(kind, identity) if identity else None
        counts = blocker_counts(state.blockers(kind, identity, path)) if identity else blocker_counts([])
        result.append({
            "kind": kind, "key": name, "identity": identity, "path": str(path.resolve()), "bytes": size,
            "bytes_truncated": truncated, "last_used_at": used.isoformat() if used else None,
            "last_task_activity": activity.isoformat() if activity else None,
            "active_tasks": counts["active"], "blockers": counts,
            "orphan": identity is None or (activity is None and not state.members(kind, identity)),
        })
    return result


def _remove(log_dir: Path, kind: str, key: str, path: Path) -> Path:
    """Rename into `.trash` (the caller holds the lock), drop the metadata and the lock file; returns the
    trashed path. Unlinking the lock file under the lock keeps `.locks` from growing without bound; a
    waiter that wins the old file's `flock` notices it left the path and retries (`_try_lock`)."""
    _ensure_layout(log_dir, kind)
    trashed = root(log_dir) / TRASH_DIRECTORY / f"{kind}-{key}-{int(time.time() * 1000)}"
    os.rename(path, trashed)
    for leftover in (_meta_path(log_dir, kind, key), _lock_path(log_dir, kind, key)):
        try:
            leftover.unlink()
        except FileNotFoundError:
            pass
    return trashed


def _delete_trash(trashed: Path) -> bool:
    try:
        shutil.rmtree(trashed)
        return True
    except FileNotFoundError:
        return True
    except OSError:
        log.warning("could not delete trashed shared scratch %s; the sweep retries", trashed, exc_info=True)
        return False


@task_operation
def clean(log_dir: Path, kind: str, identity: str) -> dict[str, Any]:
    """Remove one folder, or refuse with the reason. Blocking; for a person (the ctl gate).

    Unreadable metadata does not stop a person repairing the folder: the blocker checks run as usual, and
    the folder is then removed together with the metadata (the folder's name is derived from the identity
    given, so the right folder is the one named)."""
    key = key_for(kind, identity)
    path = root(log_dir) / kind / key
    with locked(log_dir, kind, identity):
        if path.is_symlink() or not os.path.lexists(path):
            raise SharedScratchError(f"no shared {kind} scratch folder exists for {identity!r}", code="shared_scratch_unknown")
        try:
            meta = _read_meta(log_dir, kind, key)
        except SharedScratchError:
            meta = None
        if meta is not None and meta["identity"] != identity:
            raise SharedScratchError(f"the folder {key} belongs to a different {kind}", code="shared_scratch_mismatch")
        busy = State(log_dir).blockers(kind, identity, path)
        if busy:
            raise SharedScratchError(
                "the folder is still referenced by " + ", ".join(f"{b['task_id']} ({b['kind']}: {b['status']})" for b in busy[:8])
                + ("..." if len(busy) > 8 else "") + "; wait for them to settle first", code="shared_scratch_in_use")
        size, truncated = directory_size(path)
        trashed = _remove(log_dir, kind, key, path)
    deleted = _delete_trash(trashed)
    return {"kind": kind, "identity": identity, "path": str(path), "bytes": size, "bytes_truncated": truncated,
            "deleted": deleted, "note": "The folder was removed." if deleted else f"The folder was moved to {trashed}; deleting it failed and the sweep will retry."}


@task_operation
def sweep(log_dir: Path, days: int, now: datetime | None = None) -> dict[str, int]:
    """The age policy: remove folders unused for more than `days` that nothing references.

    Age is the newest of the metadata's `last_used_at`, the newest observed `finished_at` among retained
    referencing records, and, for a folder with no metadata, its modification time. A referencing task
    that recovery found settled without an observed exit has no known finish: the sweep first records the
    moment it observed that settlement in the folder's metadata, and keeps the folder if it cannot.

    One snapshot of all records selects candidates; under each candidate's lock only that folder's
    members, receipts, continuations and pins are read again.
    """
    stats = {"removed": 0, "kept_in_use": 0, "kept_busy": 0, "kept_young": 0, "failed": 0}
    moment = now or _now()
    cutoff = moment - timedelta(days=days)
    state = State(log_dir)
    for kind, name, path in _entries(log_dir):
        if path.is_symlink() or not path.is_dir():
            continue
        try:
            meta = _read_meta(log_dir, kind, name)
        except SharedScratchError:
            stats["kept_in_use"] += 1
            continue
        identity = meta["identity"] if meta else None
        if identity is None:
            if kind == "group" or key_for_chain_ok(name) is False:
                stats["kept_in_use"] += 1  # cannot derive an identity; leave it for a person
                continue
            identity = name
        unobserved = [record.task_id for record in state.unobserved_settled(kind, identity)]
        if unobserved:
            try:
                recorded = observe_settlement(log_dir, kind, identity, unobserved, moment)
                meta = _read_meta(log_dir, kind, name) if recorded else None
            except SharedScratchError:
                recorded = False
            if not recorded:
                stats["kept_in_use"] += 1
                continue
        used = max((t for t in (_parse_time(meta.get("last_used_at")) if meta else None, state.last_activity(kind, identity)) if t is not None), default=None)
        if used is None:
            try:
                used = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            except OSError:
                continue
        if used >= cutoff:
            stats["kept_young"] += 1
            continue
        if state.blockers(kind, identity, path):
            stats["kept_in_use"] += 1
            continue
        try:
            with locked(log_dir, kind, identity, timeout=0.0):
                fresh = State.narrowed(state, kind, identity)
                fresh_meta = _read_meta(log_dir, kind, name) or {}
                fresh_used = max((t for t in (_parse_time(fresh_meta.get("last_used_at")), fresh.last_activity(kind, identity)) if t is not None), default=used)
                if fresh_used >= cutoff or fresh.blockers(kind, identity, path):
                    stats["kept_in_use"] += 1
                    continue
                trashed = _remove(log_dir, kind, name, path)
        except SharedScratchError as exc:
            stats["kept_busy" if exc.code == "shared_scratch_busy" else "kept_in_use"] += 1
            continue
        except OSError:
            stats["failed"] += 1
            continue
        stats["removed" if _delete_trash(trashed) else "failed"] += 1
    _sweep_trash(log_dir, moment)
    _sweep_orphan_meta(log_dir, moment)
    return stats


def key_for_chain_ok(name: str) -> bool:
    try:
        store.validate_task_id(name)
        return True
    except store.InvalidTaskId:
        return False


def _sweep_trash(log_dir: Path, now: datetime) -> None:
    base = root(log_dir) / TRASH_DIRECTORY
    try:
        names = os.listdir(base)
    except OSError:
        return
    for name in names:
        path = base / name
        try:
            if now.timestamp() - path.lstat().st_mtime > TRASH_MIN_AGE_SECONDS:
                if path.is_symlink():
                    path.unlink()
                else:
                    shutil.rmtree(path)
        except OSError:
            continue


def _sweep_orphan_meta(log_dir: Path, now: datetime) -> None:
    """Drop metadata whose folder is gone (a crash between rename and unlink).

    Metadata is written just before its folder is created, so only metadata old enough to be past
    any allocation is dropped."""
    for kind in KINDS:
        base = root(log_dir) / META_DIRECTORY / kind
        try:
            names = os.listdir(base)
        except OSError:
            continue
        for name in names:
            orphan = (name.endswith(".json") and not os.path.lexists(root(log_dir) / kind / name[:-5])) or (
                name.startswith(".") and name.endswith(".tmp"))  # a writer that died before its replace
            if orphan:
                try:
                    if now.timestamp() - (base / name).stat().st_mtime > TRASH_MIN_AGE_SECONDS:
                        (base / name).unlink()
                except OSError:
                    pass
