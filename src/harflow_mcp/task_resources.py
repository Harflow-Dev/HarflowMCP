"""Task-side use of the resource ledger: requests, scopes, pending launches and the task resolver.

`resources.py` knows names, queues and owners by id. This module is where a *task* becomes one:

* **Waiting lives in the launch receipt, never in a TaskRecord.** A task that asked for resources
  is acknowledged to its caller while its detached owner waits (`waiting_for_resources`), then
  `granted`, then spawned. Until spawn there is no record, so every reader of `record.pid` or
  `outcome_unobserved` is untouched; the pending view below is built from the receipt.
* **Scope.** A nested dispatch is under its caller's holding scope: the caller's reservation when
  its record holds one, or a scope that declared nothing when the caller runs inside a workflow
  attempt (stage B; the workflow stage widens that). Such a dispatch may only borrow, never queue.
* **Resolver.** `task_resolver` is what the reaper asks about a `task` entry. It decides from
  durable receipt and record state by task id, never from a post-spawn attach, and keeps the
  entry on every uncertain answer.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import identity, store
from .resources import (
    KEEP, NEVER_STARTED, SETTLED, Entry, ResourceError, ResourceLedger, ResourceNotDeclaredByScope, Scope,
    validate_names,
)

DEFAULT_WAIT_SECONDS = 3600
MAX_WAIT_SECONDS = 21600
CANCEL_SUFFIX = ".launch-cancel.json"
PENDING_STATES = ("waiting_for_resources", "granted")


def is_pending_kind(receipt: dict[str, Any] | None) -> bool:
    """A launch that may have no record for a while: it asked for resources or for a worktree."""
    return receipt is not None and (isinstance(receipt.get("resources"), dict) or isinstance(receipt.get("worktree"), dict))


def ledger_for(log_dir: Path) -> ResourceLedger:
    return ResourceLedger(log_dir.parent)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---- requests and scopes -----------------------------------------------------------------------


def validate_request(names: Any, wait_seconds: Any) -> tuple[list[str], int]:
    """`(sorted names, wait seconds)` or a ValueError the caller reports as invalid params."""
    try:
        wanted = list(validate_names(names))
    except ResourceError as exc:
        raise ValueError(str(exc)) from None
    if wait_seconds is None:
        wait_seconds = DEFAULT_WAIT_SECONDS
    if type(wait_seconds) is not int or not 0 <= wait_seconds <= MAX_WAIT_SECONDS:
        raise ValueError(f"resource_wait_seconds must be an integer from 0 to {MAX_WAIT_SECONDS}")
    return wanted, wait_seconds


def holding_scope(log_dir: Path, caller: Any) -> Scope | None:
    """The scope a detected caller task puts a nested dispatch under, or None for a root dispatch.

    A caller running inside a workflow attempt always holds a scope (its attempt holds a checkout
    lease), even with no named resources, so it refuses every name until the workflow stage lets
    attempts declare some. Otherwise the caller's own held reservation lends; a caller holding
    nothing is no scope, and the dispatch queues like any other root request.
    """
    if caller is None:
        return None
    task_id = caller.record.task_id
    if (log_dir.parent / "workflow-owners" / f"{task_id}.json").exists():
        return Scope(None)
    held = getattr(caller.record, "resources", None)
    if isinstance(held, dict) and held.get("state") == "held" and isinstance(held.get("reservation_id"), str):
        return Scope(held["reservation_id"])
    return None


def build_request(
    log_dir: Path, names: Any, wait_seconds: Any, caller: Any, *, borrowed_from: str | None = None
) -> dict[str, Any]:
    """The JSON-safe request the owner acts on, refused here when a borrow cannot be satisfied.

    `borrowed_from` is a continuation's original lender: it keeps a continuation under the scope
    its source ran in while that lender still holds, so it can never wait on a parent that is
    itself waiting on it.
    """
    wanted, wait = validate_request(names, wait_seconds)
    scope = holding_scope(log_dir, caller)
    if scope is None and borrowed_from is not None:
        lender = ledger_for(log_dir).get(borrowed_from)
        if lender is not None and lender.held and not lender.release_requested:
            scope = Scope(borrowed_from)
    if scope is not None:
        ledger_for(log_dir).check_borrow(wanted, scope)
    return {"names": wanted, "wait_seconds": wait,
            "scope": {"reservation_id": scope.reservation_id} if scope is not None else None}


def inherited_request(log_dir: Path, held: Any, caller: Any) -> dict[str, Any] | None:
    """A resume or continuation re-queues what its source asked for; it never inherits a hold."""
    if not isinstance(held, dict) or not held.get("names"):
        return None
    borrowed = held.get("borrowed_from")
    return build_request(log_dir, held["names"], held.get("wait_seconds"), caller,
                         borrowed_from=borrowed if isinstance(borrowed, str) else None)


# ---- pre-spawn cancellation --------------------------------------------------------------------


def cancel_path(log_dir: Path, task_id: str) -> Path:
    return log_dir / f"{store.validate_task_id(task_id)}{CANCEL_SUFFIX}"


def request_cancel(log_dir: Path, task_id: str) -> None:
    """Ask a launch that has not spawned to stop. Separate from the signal protocol: nothing is
    running to signal, and the owner's wait loop is the only reader."""
    from .task_supervision import _write
    _write(cancel_path(log_dir, task_id), {"task_id": task_id, "requested_at": _now()})


def cancel_requested(log_dir: Path, task_id: str) -> bool:
    return cancel_path(log_dir, task_id).exists()


def clear_cancel(log_dir: Path, task_id: str) -> None:
    cancel_path(log_dir, task_id).unlink(missing_ok=True)


def pending_descendants(log_dir: Path, root_id: str) -> list[str]:
    """Launches with no record yet that descend from `root_id` by `spawned_by` or `root_task_id`."""
    from .task_supervision import reservations
    pending = {r["task_id"]: r for r in reservations(log_dir)
               if is_pending_kind(r) and r["state"] in (*PENDING_STATES, "reserved", "claimed")
               and pending_receipt(log_dir, r["task_id"]) is not None}
    found: set[str] = set()
    changed = True
    while changed:
        changed = False
        for task_id, receipt in pending.items():
            if task_id in found or task_id == root_id:
                continue
            if receipt.get("spawned_by") in found | {root_id} or receipt.get("root_task_id") == root_id:
                found.add(task_id)
                changed = True
    return sorted(found)


def settle_cancelled(log_dir: Path, receipt: dict[str, Any]) -> None:
    """Settle a launch whose owner is gone: it provably never started, so nothing waits to hear it."""
    from .task_supervision import mark
    held = receipt.get("resources") or {}
    release_quietly(log_dir, held.get("reservation_id"))
    mark(log_dir, receipt["task_id"], receipt["generation"], state="not_started", not_started_reason="cancelled",
         error="cancelled before the task started", resources={**held, "state": "not_started"})
    clear_cancel(log_dir, receipt["task_id"])


def cancel_launch(log_dir: Path, task_id: str, *, authorized: bool = False) -> dict[str, Any]:
    """Ask a launch, and every pending descendant, to stop before it spawns. Blocking.

    Internal: it performs no caller authorization. Every entry point must come through
    `TaskRegistry.cancel_pending` (or the cascade, which has already authorized its caller), which
    applies the checks `cancel_cascade` applies to recorded tasks; `authorized=True` is that
    caller's statement, and a call without it is refused so a new entry point cannot skip them.

    A launch whose waiting owner is confirmed dead is settled here, since nothing else will.
    `spawned` is true when a record appeared in the meantime: the caller then runs the normal cancel.
    """
    from .task_supervision import read_launch, read_task_record
    if authorized is not True:
        raise PermissionError("cancel_launch requires an authorized caller; use TaskRegistry.cancel_pending")
    targets = [task_id, *pending_descendants(log_dir, task_id)]
    requested, settled = [], []
    for target in targets:
        receipt = pending_receipt(log_dir, target)
        if receipt is None:
            continue
        request_cancel(log_dir, target)
        requested.append(target)
        if receipt["state"] == "waiting_for_resources" and _owner_verdict(receipt) == "dead":
            settle_cancelled(log_dir, receipt)
            settled.append(target)
    spawned = read_task_record(log_dir, task_id, include_prompt=False) is not None
    return {"spawned": spawned, "cascade": {"pending_cancel_requested": requested, "pending_settled": settled}}


# ---- the pending view --------------------------------------------------------------------------


def _owner_verdict(receipt: dict[str, Any]) -> str:
    owner = receipt.get("owner")
    if owner is None:
        return "undecidable"
    try:
        return identity.identity_check(owner)
    except Exception:
        return "undecidable"


def _queue_details(log_dir: Path, receipt: dict[str, Any]) -> dict[str, Any]:
    held = receipt.get("resources") or {}
    names = held.get("names") or []
    details: dict[str, Any] = {"names": names, "reservation_id": held.get("reservation_id"),
                               "wait_started_at": held.get("wait_started_at"), "deadline_at": held.get("deadline_at"),
                               "granted_at": held.get("granted_at"), "wait_seconds": held.get("wait_seconds")}
    try:
        entries = ledger_for(log_dir).entries()
    except ResourceError as exc:
        details["queue_state"] = {"unavailable": exc.code}
        return details
    mine = next((entry for entry in entries if entry.reservation_id == held.get("reservation_id")), None)
    blocked_by = []
    for name in names:
        holders = [entry for entry in entries if entry.held and entry.borrows_from is None and name in entry.names]
        if mine is not None and mine.held:
            holders = []
        if holders:
            blocked_by.append({
                "name": name,
                "holder_task_ids": sorted({e.owner.task_id for e in holders if e.owner.task_id}),
                "holder_workflow_run_ids": sorted({e.owner.workflow_run_id for e in holders if e.owner.workflow_run_id}),
                "since": min(e.granted_at or e.enqueued_at for e in holders),
            })
    details["blocked_by"] = blocked_by
    if mine is not None and not mine.held:
        ahead = [e for e in entries if not e.held and e.enqueued_at <= mine.enqueued_at and e is not mine
                 and not set(e.names).isdisjoint(mine.names)]
        details["position"] = len(ahead) + 1
    return details


def view(log_dir: Path, receipt: dict[str, Any], *, check_owner: bool = False) -> dict[str, Any]:
    """The MCP/ctl-facing state of a launch that has no TaskRecord. Blocking when `check_owner`."""
    state = receipt["state"]
    held = receipt.get("resources") or {}
    resources = _queue_details(log_dir, receipt) if state in PENDING_STATES else {
        key: held.get(key) for key in ("names", "reservation_id", "wait_started_at", "deadline_at", "granted_at")}
    payload: dict[str, Any] = {
        "task_id": receipt["task_id"], "backend": receipt["backend"], "session_id": receipt.get("session_id"),
        "repo_path": receipt.get("repo_path"), "freedom": receipt.get("freedom"), "title": receipt.get("title"),
        "group": receipt.get("group"), "parent_task_id": receipt.get("parent_task_id"),
        "spawned_by": receipt.get("spawned_by"), "root_task_id": receipt.get("root_task_id"),
        "started_at": receipt.get("created_at"), "pending_launch": True, "recovered": False,
        "launch_state": state, "status": "running",
        "notices": [], "enforcement": _nominal_enforcement(receipt),
    }
    if isinstance(receipt.get("resources"), dict):
        payload["resources"] = {"state": "waiting" if state == "waiting_for_resources" else state, **resources}
    if isinstance(receipt.get("worktree"), dict):
        payload["worktree"] = dict(receipt["worktree"])
    payload["enforcement_note"] = "Nominal for the requested freedom; the spawned task reports what was actually enforced."
    if state == "not_started":
        cancelled = receipt.get("not_started_reason") == "cancelled"
        payload.update(status="cancelled" if cancelled else "failed", not_started=True,
                       not_started_reason=receipt.get("not_started_reason"), detail=receipt.get("error"))
        if "resources" in payload:
            payload["resources"]["state"] = "not_started"
    elif state in PENDING_STATES or state in ("reserved", "claimed"):
        verdict = _owner_verdict(receipt) if check_owner else "alive"
        if verdict == "undecidable":
            payload["launch_state"] = "uncertain"
            payload["notices"].append("The launch owner cannot be inspected; its resources are kept until it can be settled.")
        elif verdict == "dead":
            worktree_state = (receipt.get("worktree") or {}).get("state")
            if worktree_state == "creating":
                payload["worktree"]["state"] = "unknown"  # the owner died inside the Git command that writes
                payload["notices"].append("The owner died while creating the worktree; its state is unknown and nothing was removed.")
            if state == "granted" and worktree_state not in ("planned", "creating"):
                payload["launch_state"] = "uncertain"
                payload["notices"].append("The launch owner is gone after the resources were granted and before a task record "
                                          "appeared; the resources are kept. A person can release them with `harflow-ctl resources-release`.")
            else:
                payload.update(status="failed", not_started=True, not_started_reason="owner_died")
        if payload["status"] == "running":
            names = ", ".join(held.get("names") or [])
            payload["next_step"] = (f"waiting for resources ({names}); call wait_for_task again or poll get_task_status"
                                    if state == "waiting_for_resources" else
                                    "resources granted; the task is about to start. Call wait_for_task or poll get_task_status")
    return payload


def _nominal_enforcement(receipt: dict[str, Any]) -> dict[str, Any]:
    from . import backends
    try:
        return backends.get(receipt["backend"]).enforcement(receipt.get("freedom") or "write_in_repo", receipt.get("network")).as_dict()
    except Exception:
        return {}


def pending_receipt(log_dir: Path, task_id: str) -> dict[str, Any] | None:
    """The launch receipt of a task with no record yet, or None."""
    from .task_supervision import read_launch, read_task_record
    receipt = read_launch(log_dir, task_id)
    if receipt is None or read_task_record(log_dir, task_id, include_prompt=False) is not None:
        return None
    return receipt


def pending_view(log_dir: Path, task_id: str, *, check_owner: bool = True) -> dict[str, Any] | None:
    receipt = pending_receipt(log_dir, task_id)
    if not is_pending_kind(receipt):
        return None  # a launch without resources or a worktree is only a handshake in flight, not a pending task
    return view(log_dir, receipt, check_owner=check_owner)


def pending_views(log_dir: Path, *, check_owner: bool = False) -> list[dict[str, Any]]:
    from .task_supervision import reservations
    found = []
    for receipt in reservations(log_dir):
        if is_pending_kind(receipt) and receipt["state"] not in ("spawned", "settled"):
            if pending_receipt(log_dir, receipt["task_id"]) is not None:
                found.append(view(log_dir, receipt, check_owner=check_owner))
    return found


class _PendingDone:
    def __init__(self, handle: "PendingLaunch") -> None:
        self.handle = handle

    def is_set(self) -> bool:
        return self.handle.resolved()

    async def wait(self) -> bool:
        """Until the launch spawned and settled, or provably never started."""
        task = None
        while True:
            if task is None:
                from .task_supervision import read_task_record
                if await asyncio.to_thread(read_task_record, self.handle.log_dir, self.handle.task_id) is not None:
                    from .task_attachment import AttachedTask
                    task = AttachedTask(self.handle.log_dir, self.handle.task_id)
            if task is not None:
                return await task.done.wait()
            if await asyncio.to_thread(self.handle.resolved):
                return True
            await asyncio.sleep(0.5)


class PendingLaunch:
    """What `task_supervision.launch` returns while the owner waits for resources.

    It is a read-only handle on the launch receipt with just enough of a task's surface (`task_id`,
    `brief`, `snapshot`, `enforcement`, `done`) for every start path to acknowledge it. It is never
    a record and never a source of metadata writes.
    """

    def __init__(self, log_dir: Path, receipt: dict[str, Any]) -> None:
        self.log_dir = log_dir
        self.task_id = receipt["task_id"]
        self.launch_state = receipt["state"]
        self.receipt = receipt
        self.done = _PendingDone(self)
        self.latest_record_landed = False
        self.persistence_error = None

    @property
    def enforcement(self) -> dict[str, Any]:
        return _nominal_enforcement(self.receipt)

    @property
    def status(self) -> str:
        return "running"

    def _fresh(self) -> dict[str, Any]:
        from .task_supervision import read_launch
        return read_launch(self.log_dir, self.task_id) or self.receipt

    def brief(self) -> dict[str, Any]:
        return view(self.log_dir, self._fresh())

    def snapshot(self) -> dict[str, Any]:
        return self.brief()

    def resolved(self) -> bool:
        from .task_supervision import read_launch, read_task_record
        if read_task_record(self.log_dir, self.task_id) is not None:
            return False  # spawned: the record's own settlement decides, via AttachedTask
        receipt = read_launch(self.log_dir, self.task_id)
        if receipt is None or receipt["state"] in ("not_started", "settled"):
            return True
        return receipt["state"] == "waiting_for_resources" and _owner_verdict(receipt) == "dead"


# ---- ledger release ----------------------------------------------------------------------------


def release_quietly(log_dir: Path, reservation_id: str | None) -> bool:
    """Release a reservation on a path where bookkeeping must not change an outcome."""
    if not reservation_id:
        return False
    try:
        ledger_for(log_dir).release(reservation_id)
        return True
    except ResourceError:
        return False
    except OSError:
        return False


# ---- the task resolver -------------------------------------------------------------------------


def task_resolver(log_dir: Path):
    """The reaper's answer for a `task` entry (settled, provably never started, or keep)."""
    from . import retention
    from .task_supervision import read_launch, read_task_record

    def resolve(entry: Entry) -> str:
        task_id = entry.owner.task_id
        if not task_id:
            return KEEP
        record = read_task_record(log_dir, task_id, include_prompt=False)
        if record is not None:
            status = store.resolve_status(log_dir, record, detail=False)[0]
            return SETTLED if retention._conclusively_settled(record, status) else KEEP
        receipt = read_launch(log_dir, task_id)
        if receipt is None:
            return KEEP
        if receipt["state"] == "not_started":
            return NEVER_STARTED
        if receipt["state"] == "waiting_for_resources" and _owner_verdict(receipt) == "dead":
            return NEVER_STARTED
        if (receipt["state"] == "granted" and (receipt.get("worktree") or {}).get("state") in ("planned", "creating")
                and _owner_verdict(receipt) == "dead"):
            return NEVER_STARTED  # spawn follows `created`: the agent was never started
        # `granted` without a record, an owner that cannot be inspected, and anything else: only a
        # person's `resources-release` frees these.
        return KEEP

    return resolve


def reap(log_dir: Path):
    """Run the reaper with the task resolver; the only owner kind with one in this stage."""
    return ledger_for(log_dir).reap({"task": task_resolver(log_dir)})


# ---- recovery commands -------------------------------------------------------------------------


def release_session_claim(log_dir: Path, task_id: str) -> str:
    """Settle a `granted` launch's session claim after a person released its reservation.

    Only a launch with no record and still `granted` is touched; its owner is asked nothing, the
    person already decided. Returns `released`, `not_applicable` or `unknown`.
    """
    from .task_supervision import mark, read_launch, read_task_record
    receipt = read_launch(log_dir, task_id)
    if receipt is None:
        return "unknown"
    if receipt["state"] not in ("granted", "waiting_for_resources") or read_task_record(log_dir, task_id, include_prompt=False) is not None:
        return "not_applicable"
    mark(log_dir, task_id, receipt["generation"], state="not_started", not_started_reason="released_by_person",
         error="released with harflow-ctl resources-release")
    return "released"


def rebuild(log_dir: Path) -> dict[str, Any]:
    """Re-create held ledger entries from durable launch receipts and records, where provable.

    Provable means: a `granted` launch with no record (its owner may be about to spawn), or a
    record whose `resources` is `held` and whose run is not conclusively settled. Waiting launches
    are *not* restored (a waiting owner re-enqueues nothing, and a dead one never started). An entry
    that conflicts with an already restored holder is reported, not forced.
    """
    from . import retention
    from .task_supervision import read_task_record, reservations
    ledger = ledger_for(log_dir)
    restored: list[str] = []
    conflicts: list[dict[str, Any]] = []
    seen: set[str] = set()

    def attempt(task_id: str, held: dict[str, Any]) -> None:
        rid = held.get("reservation_id")
        if not isinstance(rid, str) or rid in seen:
            return
        seen.add(rid)
        try:
            entry = ledger.restore_held(held["names"], {"kind": "task", "task_id": task_id}, rid)
        except ResourceError as exc:
            conflicts.append({"task_id": task_id, "reservation_id": rid, "reason": exc.code})
            return
        if entry is None:
            conflicts.append({"task_id": task_id, "reservation_id": rid, "reason": "name_already_held"})
        else:
            restored.append(rid)

    for record in store.read_all(log_dir):
        held = getattr(record, "resources", None)
        if not isinstance(held, dict) or held.get("state") != "held" or held.get("borrowed_from"):
            continue
        status = store.resolve_status(log_dir, record, detail=False)[0]
        if not retention._conclusively_settled(record, status):
            attempt(record.task_id, held)
    for receipt in reservations(log_dir):
        held = receipt.get("resources")
        if (isinstance(held, dict) and receipt["state"] == "granted" and not held.get("scope")
                and read_task_record(log_dir, receipt["task_id"], include_prompt=False) is None):
            attempt(receipt["task_id"], held)
    return {"restored": restored, "conflicts": conflicts}
