"""Task-side use of the resource ledger: requests, scopes, pending launches and the task resolver.

`resources.py` knows names, queues and owners by id. This module is where a *task* becomes one:

* **Waiting lives in the launch receipt, never in a TaskRecord.** A task that asked for resources
  is acknowledged to its caller while its detached owner waits (`waiting_for_resources`), then
  `granted`, then spawned. Until spawn there is no record, so every reader of `record.pid` or
  `outcome_unobserved` is untouched; the pending view below is built from the receipt.
* **Scope.** A nested dispatch is under the nearest holding scope on its caller's `spawned_by` chain:
  an ancestor's reservation, or a scope that declared nothing when an ancestor runs inside a workflow
  attempt (including its run and attempt resource coverage). Such a dispatch may only borrow, never queue.
* **Resolver.** `task_resolver` is what the reaper asks about a `task` entry. It decides from
  durable receipt and record state by task id, never from a post-spawn attach, and keeps the
  entry on every uncertain answer.
"""
from __future__ import annotations

import asyncio
from collections.abc import Iterable
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
# Set on a `not_started` receipt whose handed-over follow-ups may have reached an agent.
UNCERTAIN_FOLLOWUPS_KEY = "followups_delivery_unknown"


def is_pending_kind(receipt: dict[str, Any] | None) -> bool:
    """A launch that may have no record for a while: it asked for resources or for a worktree."""
    return receipt is not None and (isinstance(receipt.get("resources"), dict) or isinstance(receipt.get("worktree"), dict))


def ledger_for(log_dir: Path) -> ResourceLedger:
    return ResourceLedger(log_dir.parent)


class LaunchCancelled(RuntimeError):
    """A pending launch's cancel request was honoured at the last point before the agent starts."""


def ledger_entry_visible(log_dir: Path, receipt: dict[str, Any]) -> bool:
    """Whether the ledger already has the entry a `waiting_for_resources` receipt names.

    A launch that declared no resources has nothing to wait for and is visible by definition. A
    ledger that cannot be read is not an acknowledgement; the owner settles such a launch itself.
    """
    held = receipt.get("resources")
    if not isinstance(held, dict) or not held.get("reservation_id"):
        return True
    try:
        return ledger_for(log_dir).get(held["reservation_id"]) is not None
    except ResourceError:
        return False


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


MAX_SCOPE_DEPTH = 64


def _scope_of(log_dir: Path, task_id: str, record: Any, receipt: dict[str, Any] | None) -> Scope | None:
    """The scope one task holds, or None when it holds nothing itself."""
    if (log_dir.parent / "workflow-owners" / f"{task_id}.json").exists():
        from .workflow_receipt_io import read
        from .workflow_resources import scope_for_task
        try:
            owner, _source = read(log_dir.parent / "workflow-owners" / f"{task_id}.json")
            return scope_for_task(log_dir, owner)
        except Exception:
            # An unqualified receipt still proves a workflow boundary, never names
            # it may lend. The empty scope retains the pre-integration refusal.
            return Scope(None)
    held = getattr(record, "resources", None) if record is not None else (receipt or {}).get("resources")
    if isinstance(held, dict) and held.get("state") in ("held", "granted") and isinstance(held.get("reservation_id"), str):
        return Scope(held["reservation_id"])
    return None


def holding_scope(log_dir: Path, caller: Any) -> Scope | None:
    """The scope a detected caller task puts a nested dispatch under, or None for a root dispatch.

    The scope is the nearest holder on the caller's `spawned_by` chain, the caller included: a task
    that owns a `workflow-owners` receipt (a workflow attempt always holds a checkout lease, so it is
    a scope even with no named resources and then refuses every name), or one whose record or pending launch holds a reservation. A caller with no holder
    anywhere above it is no scope, and the dispatch queues like any other root request. Walking the
    chain is what stops a non-holding intermediate from hiding its ancestors' hold and letting a
    grandchild queue behind a lender that is waiting on it. An ancestor that cannot be read, or a chain
    that does not end, fails closed.
    """
    from .task_supervision import read_launch, read_task_record
    if caller is None:
        return None
    record, task_id = caller.record, caller.record.task_id
    receipt = None
    seen: set[str] = set()
    while True:
        if task_id in seen or len(seen) >= MAX_SCOPE_DEPTH:
            raise ResourceNotDeclaredByScope("the dispatch lineage is cyclic or too deep to establish an enclosing scope; refused")
        seen.add(task_id)
        scope = _scope_of(log_dir, task_id, record, receipt)
        if scope is not None:
            return scope
        parent = getattr(record, "spawned_by", None) if record is not None else (receipt or {}).get("spawned_by")
        if not parent:
            return None
        try:
            record = read_task_record(log_dir, parent, include_prompt=False)
            receipt = read_launch(log_dir, parent) if record is None else None
        except (store.TaskDocumentError, OSError, ValueError) as exc:
            raise ResourceNotDeclaredByScope(f"the enclosing task {parent} cannot be read ({exc}); refused") from None
        if record is None and receipt is None:
            raise ResourceNotDeclaredByScope(f"the enclosing task {parent} cannot be found, so no enclosing scope can be established; refused")
        task_id = parent


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


def pending_descendants(log_dir: Path, root_id: str, seeds: Iterable[str] = ()) -> list[str]:
    """Launches with no record yet that descend from `root_id` by `spawned_by` or `root_task_id`.

    `seeds` are recorded descendants already known to belong to `root_id` (the cascade's own targets):
    a pending launch under one of them descends from `root_id` even though no pending receipt links them.
    """
    from .task_supervision import reservations
    pending = {r["task_id"]: r for r in reservations(log_dir)
               if is_pending_kind(r) and r["state"] in (*PENDING_STATES, "reserved", "claimed")
               and pending_receipt(log_dir, r["task_id"]) is not None}
    found: set[str] = set()
    known = {root_id, *seeds}
    changed = True
    while changed:
        changed = False
        for task_id, receipt in pending.items():
            if task_id in found or task_id == root_id:
                continue
            if receipt.get("spawned_by") in found | known or receipt.get("root_task_id") == root_id:
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
        # A request is only worth writing for a launch an owner can still read it for; a settled
        # one would keep the file forever.
        if receipt is None or receipt["state"] not in (*PENDING_STATES, "reserved", "claimed"):
            continue
        request_cancel(log_dir, target)
        requested.append(target)
        if receipt["state"] == "waiting_for_resources":
            fresh = owner_confirmed_gone(log_dir, receipt)
            if fresh is not None:
                settle_cancelled(log_dir, fresh)
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


def reread_pending(log_dir: Path, receipt: dict[str, Any], states: tuple[str, ...]) -> dict[str, Any] | None:
    """The current receipt when it is still in one of `states` with no record, else None.

    For use after an owner was confirmed dead: the probe can be slow, and the owner may have
    granted, spawned and died while it ran, so a verdict computed from the receipt read before it
    is stale. A dead owner changes nothing further, so what this reads is final.
    """
    from .task_supervision import read_launch, read_task_record
    fresh = read_launch(log_dir, receipt["task_id"])
    if fresh is None or fresh["generation"] != receipt["generation"] or fresh["state"] not in states:
        return None
    if read_task_record(log_dir, receipt["task_id"], include_prompt=False) is not None:
        return None
    return fresh


def owner_confirmed_gone(log_dir: Path, receipt: dict[str, Any], states: tuple[str, ...] = ("waiting_for_resources",)) -> dict[str, Any] | None:
    """The fresh receipt when the launch is in `states`, its owner is dead and, re-read after that
    probe, it is still there with no record; otherwise None (keep it)."""
    if receipt["state"] not in states or _owner_verdict(receipt) != "dead":
        return None
    return reread_pending(log_dir, receipt, states)


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
        "notices": [], "enforcement": receipt.get("enforcement"),
    }
    if isinstance(receipt.get("resources"), dict):
        payload["resources"] = {"state": "waiting" if state == "waiting_for_resources" else state, **resources}
    if isinstance(receipt.get("worktree"), dict):
        payload["worktree"] = dict(receipt["worktree"])
    if receipt.get("enforcement") is not None:
        payload["enforcement_note"] = "Planned by the caller before the launch; the spawned task reports what was actually enforced."
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
        elif verdict == "dead" and (fresh := reread_pending(log_dir, receipt, (state,))) is not None:
            receipt = fresh
            worktree_state = (receipt.get("worktree") or {}).get("state")
            if worktree_state == "creating":
                payload["worktree"]["state"] = "unknown"  # the owner died inside the Git command that writes
                payload["notices"].append("The owner died while creating the worktree; its state is unknown and nothing was removed.")
            if state == "granted" and worktree_state not in ("planned", "creating"):
                payload["launch_state"] = "uncertain"
                payload["notices"].append("The launch owner is gone after the launch was granted and before a task record "
                                          "appeared, so whether the agent started is unknown; its resources are kept. A person who has "
                                          f"checked can settle it with `harflow-ctl launch-settle --task-id {receipt['task_id']} --confirm-uncertain`.")
            else:
                payload.update(status="failed", not_started=True, not_started_reason="owner_died")
        if payload["status"] == "running":
            names = ", ".join(held.get("names") or [])
            payload["next_step"] = (f"waiting for resources ({names}); call wait_for_task again or poll get_task_status"
                                    if state == "waiting_for_resources" else
                                    "resources granted; the task is about to start. Call wait_for_task or poll get_task_status")
    return payload


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
    def enforcement(self) -> dict[str, Any] | None:
        """The caller's planned enforcement from the receipt, or None when the launch recorded none."""
        return self._fresh().get("enforcement")

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
        # A dead owner that never recorded a spawn is resolved too: waiting means it never started,
        # granted means nobody can tell, and either way nothing will wake a waiter later.
        return owner_confirmed_gone(self.log_dir, receipt, PENDING_STATES) is not None


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
        if receipt["state"] == "waiting_for_resources" and owner_confirmed_gone(log_dir, receipt) is not None:
            return NEVER_STARTED
        if receipt["state"] == "granted" and (fresh := owner_confirmed_gone(log_dir, receipt, ("granted",))) is not None:
            if (fresh.get("worktree") or {}).get("state") in ("planned", "creating"):
                return NEVER_STARTED  # spawn follows `created`: the agent was never started
        # `granted` without a record, an owner that cannot be inspected, and anything else: only a
        # person's `resources-release` frees these.
        return KEEP

    return resolve


def reap(log_dir: Path):
    """Run the reaper with task, workflow run and workflow attempt resolvers."""
    from .workflow_resources import owner_resolvers
    return ledger_for(log_dir).reap({"task": task_resolver(log_dir), **owner_resolvers(log_dir)})


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
    changes: dict[str, Any] = {}
    if receipt["state"] == "granted" and receipt.get("followups") and (receipt.get("worktree") or {}).get("state") not in ("planned", "creating"):
        changes[UNCERTAIN_FOLLOWUPS_KEY] = True  # the agent may have started: its follow-ups must not return to the queue
    mark(log_dir, task_id, receipt["generation"], state="not_started", not_started_reason="released_by_person",
         error="released with harflow-ctl resources-release", **changes)
    return "released"


class LaunchSettleRefused(ValueError):
    """`settle_uncertain` declined; `code` is the stable reason."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def settle_uncertain(log_dir: Path, task_id: str) -> dict[str, Any]:
    """Settle a launch with no record whose owner is gone after it was granted. Blocking; humans only.

    Whether the agent started is unknowable here, so a person decides. The caller (the ctl command)
    has already applied the humans-only gate. Only a `granted` launch with no record and a confirmed
    dead owner is touched. It releases every ledger entry the task owns, its session claim and its
    worktree usage (all by settling the receipt `not_started`), and finalizes the follow-ups it was
    handed: back to the queue only when the agent provably never started (the owner died while the
    worktree was still being planned or made), otherwise `delivery_unknown`. It works from the receipt
    alone, so a reservation that was released earlier with `resources-release` does not get in its way.
    """
    from . import followups
    from .task_supervision import mark, read_launch, read_task_record
    receipt = read_launch(log_dir, task_id)
    if receipt is None:
        raise LaunchSettleRefused("unknown_launch", f"no launch is recorded for task {task_id}")
    if read_task_record(log_dir, task_id, include_prompt=False) is not None:
        raise LaunchSettleRefused("launch_has_record", f"task {task_id} has a task record; stop or inspect that task instead")
    if receipt["state"] != "granted":
        raise LaunchSettleRefused("launch_not_uncertain", f"launch {task_id} is {receipt['state']}, not an uncertain granted launch")
    fresh = owner_confirmed_gone(log_dir, receipt, ("granted",))
    if fresh is None:
        raise LaunchSettleRefused("launch_owner_not_dead", f"the owner of launch {task_id} is not confirmed dead, or the launch changed; try again later")
    worktree = dict(fresh.get("worktree") or {})
    provably_not_started = worktree.get("state") in ("planned", "creating")
    released = []
    ledger = ledger_for(log_dir)
    wanted = {(fresh.get("resources") or {}).get("reservation_id")}
    wanted |= {e.reservation_id for e in ledger.entries() if e.owner.task_id == task_id}
    for reservation_id in sorted(r for r in wanted if r):
        if ledger.release(reservation_id) != "absent":
            released.append(reservation_id)
    handed = fresh.get("followups")
    outcome = None
    changes: dict[str, Any] = {"state": "not_started", "not_started_reason": "settled_by_person",
                               "error": "settled with harflow-ctl launch-settle"}
    if worktree.get("state") == "creating":
        changes["worktree"] = worktree | {"state": "unknown"}
    if handed:
        outcome = "released" if provably_not_started else "delivery_unknown"
        if not provably_not_started:
            # Recorded in the receipt before the ledger is written: if that write fails, or this process
            # stops between the two, reconciliation still reads the uncertainty from the receipt.
            changes[UNCERTAIN_FOLLOWUPS_KEY] = True
    mark(log_dir, task_id, fresh["generation"], **changes)
    clear_cancel(log_dir, task_id)
    if handed:
        followups.finish(log_dir, handed["task_id"], task_id, handed["ids"], outcome,
                         None if provably_not_started else "the launch owner stopped after the launch was granted; a person settled it")
    return {"task_id": task_id, "outcome": "never_started" if provably_not_started else "uncertain",
            "released_reservations": released, "followups": outcome}


def _held_task_spec(task_id: str, held: dict[str, Any]) -> dict[str, Any]:
    return {"reservation_id": held["reservation_id"], "names": list(held["names"]), "owner": {"kind": "task", "task_id": task_id}}


def collect_evidence(log_dir: Path) -> dict[str, Any]:
    """The ledger entries that durable launch receipts and task records prove, as repair specs.

    Provable means: a record whose `resources` is `held` and whose run is not conclusively settled,
    or a `granted` launch with no record (its owner may be about to spawn). An owner granted a name
    while its receipt still said `waiting_for_resources` is not found here: it confirms its grant
    after publishing `granted` and requests it again if a repair dropped it. Roots come first; a
    borrower is recorded with its lender, and the lender is kept while any borrower is unresolved:
    from its own record when that is readable (flagged `release_requested` once the lender itself
    settled), or as a conservative entry that blocks the borrowers' names when the lender cannot be
    found, because freeing it early would break the guarantee the borrow was granted under. Waiting
    launches are not restored (a live waiting owner finds its entry gone and settles `not_started`;
    a dead one never started). Pure reads; nothing here takes the ledger lock.
    """
    from . import retention
    from .task_supervision import reservations
    roots: dict[str, dict[str, Any]] = {}
    borrowers: list[dict[str, Any]] = []
    by_reservation: dict[str, tuple[str, dict[str, Any]]] = {}
    seen: set[str] = set()

    def note(task_id: str, held: dict[str, Any], lender: str | None) -> None:
        rid = held.get("reservation_id")
        if not isinstance(rid, str) or rid in seen or not isinstance(held.get("names"), list):
            return
        seen.add(rid)
        spec = _held_task_spec(task_id, held)
        if lender:
            borrowers.append(spec | {"borrows_from": lender})
        else:
            roots[rid] = spec

    # Receipts are read before records. A launch that spawns in between has a record by the time the
    # records are read, so it is found there; one that spawns later is still `granted` with no record
    # in what was read first. Reading records first would let that launch fall between the two.
    snapshot = reservations(log_dir)
    records = list(store.read_all(log_dir))
    recorded_ids = {record.task_id for record in records}
    for record in records:
        held = getattr(record, "resources", None)
        if not isinstance(held, dict) or not isinstance(held.get("reservation_id"), str):
            continue
        by_reservation.setdefault(held["reservation_id"], (record.task_id, held))
        if held.get("state") != "held":
            continue
        status = store.resolve_status(log_dir, record, detail=False)[0]
        if not retention._conclusively_settled(record, status):
            note(record.task_id, held, held.get("borrowed_from") if isinstance(held.get("borrowed_from"), str) else None)
    for receipt in snapshot:
        held = receipt.get("resources")
        if isinstance(held, dict) and receipt["state"] == "granted" and receipt["task_id"] not in recorded_ids:
            scope = held.get("scope") if isinstance(held.get("scope"), dict) else {}
            lender = held.get("borrowed_from") or scope.get("reservation_id")
            note(receipt["task_id"], held, lender if isinstance(lender, str) else None)

    from .workflow_resources import collect_evidence as workflow_evidence
    workflow_specs = workflow_evidence(log_dir)
    required = {spec["borrows_from"] for spec in [*borrowers, *workflow_specs] if spec.get("borrows_from") and not spec.get("release_requested")}
    while True:
        parents = {spec["borrows_from"] for spec in workflow_specs if spec["reservation_id"] in required and spec.get("borrows_from")}
        if parents <= required:
            break
        required |= parents
    for spec in workflow_specs:
        if spec.get("release_requested") and spec["reservation_id"] not in required:
            continue
        if spec.get("borrows_from"):
            borrowers.append(spec)
        else:
            roots[spec["reservation_id"]] = spec
    conflicts: list[dict[str, Any]] = []
    lenders: dict[str, list[dict[str, Any]]] = {}
    for spec in borrowers:
        lenders.setdefault(spec["borrows_from"], []).append(spec)
    for lender, group in lenders.items():
        if lender in roots:
            continue
        known = by_reservation.get(lender)
        if known is not None and isinstance(known[1].get("names"), list):
            roots[lender] = _held_task_spec(known[0], known[1] | {"reservation_id": lender}) | {"release_requested": True}
            continue
        # Nobody can say who the lender was or what it declared: block at least what its borrowers use.
        names = sorted({name for spec in group for name in spec["names"]})
        roots[lender] = {"reservation_id": lender, "names": names, "owner": {"kind": "task"}, "release_requested": True}
        conflicts.extend({"owner": spec["owner"], **({"task_id": spec["owner"]["task_id"]} if spec["owner"].get("task_id") else {}), "reservation_id": lender, "reason": "lender_unknown"} for spec in group)
    return {"specs": [*roots.values(), *borrowers], "conflicts": conflicts}


def repair(log_dir: Path) -> dict[str, Any]:
    """Repair a corrupt (or interrupted-repair) ledger from durable state, atomically and fail-closed.

    The ledger refuses every operation from `begin_repair` until the complete reconstruction is
    published, so nothing can take a name that is really held while the evidence is collected. If
    collection fails, the ledger stays closed and running this again resumes the repair.
    """
    ledger = ledger_for(log_dir)
    token = ledger.begin_repair()
    evidence = collect_evidence(log_dir)
    result = ledger.complete_repair(token, evidence["specs"])
    return {"moved_to": str(token.moved_to) if token.moved_to is not None else None,
            "restored": result.restored, "conflicts": [*evidence["conflicts"], *result.conflicts]}
