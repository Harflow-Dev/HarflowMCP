"""Durable notification points for root workflow runs.

A point records that a root run reached something a subscriber may care about: it settled
(`workflow.settled`) or needs the caller (`workflow.attention`). Points are computed inside
`WorkflowStore.update_run`, the one mutation choke point, and written atomically with the run, so
nothing depends on when a reader happens to poll. Everything here is bookkeeping: callers wrap it
so an exception never changes a mutation's outcome.

A terminal point is recorded at the transition even while Run workflow children are still
settling, so a later recovery cannot erase it. When the root has an invocation activation that
actually started, the point's identity carries `requires_tree_check: true`; the notification
worker (never update_run, which must not read child runs) then calls `tree_settlement` before
delivering it, and delivers an `unconfirmed` result with `tree_settlement_unconfirmed: true`.
"""
from __future__ import annotations

import json
import logging
from typing import Any

from . import wait_policy

log = logging.getLogger("polybridge")

TERMINAL_POINT_LIMIT = 64
ATTENTION_POINT_LIMIT = 32
SNAPSHOT_BYTE_LIMIT = 12 * 1024
QUESTION_LIMIT = 2000
REASON_LIMIT = 2000
SUMMARY_LIMIT = 4096
FAILURE_LIMIT = 2000

SETTLED = "workflow.settled"
ATTENTION = "workflow.attention"

_UNSETTLED_TASKS = {"reserved", "running", "uncertain"}

# Retained snapshot bytes per run, across both lists; older snapshots are nulled past it.
SNAPSHOT_TOTAL_BYTE_LIMIT = 256 * 1024


def is_root(run: dict[str, Any]) -> bool:
    return not run.get("parent_link")


def tasks_settling(run: dict[str, Any]) -> bool:
    """Whether any attempt is unsettled; derived from the record, never from a stored flag."""
    return any(
        task.get("status") in _UNSETTLED_TASKS
        for activation in run.get("activations", [])
        for task in activation.get("tasks", [])
    )


def has_started_invocation(run: dict[str, Any]) -> bool:
    """Whether a Run workflow activation exists whose child may have started."""
    return any(
        isinstance(activation.get("invocation"), dict) and activation.get("status") != "not_started"
        for activation in run.get("activations", [])
    )


TREE_RUN_LIMIT = 64


def _supervisor_gone(run: dict[str, Any]) -> bool:
    from . import identity
    recorded = run.get("supervisor_identity")
    if recorded:
        return identity.identity_check(recorded) == "dead"
    pid = run.get("supervisor_pid")
    if not isinstance(pid, int) or pid <= 0:
        return True
    from .workflows import _alive
    return not _alive(pid)


class _TreeCheck:
    """One bounded tree walk: shared read budget, visited set, and the root that owns the tree."""

    def __init__(self, store: Any, root: dict[str, Any]) -> None:
        from .catalog import METADATA_BYTES, MetadataBudget
        self.store = store
        self.root = root
        self.visited = {root.get("workflow_run_id")}
        self.budget = MetadataBudget()
        self.limit = METADATA_BYTES
        self._gone: bool | None = None

    def root_orphaned(self) -> bool:
        """The root is terminal and its supervisor is confirmed gone: nothing can create a child."""
        if self._gone is None:
            self._gone = self.root.get("status") in wait_policy.WORKFLOW_TERMINAL and _supervisor_gone(self.root)
        return self._gone

    def read(self, run_id: Any) -> dict[str, Any] | None:
        """The run, None when it does not exist; any other failure (size, budget) raises."""
        try:
            return self.store.get_run(run_id, metadata_byte_limit=self.limit, metadata_budget=self.budget)
        except FileNotFoundError:
            return None

    def activation_settled(self, run: dict[str, Any], activation: dict[str, Any]) -> bool:
        """The one per-activation rule, applied at every depth."""
        invocation = activation.get("invocation")
        if not isinstance(invocation, dict) or activation.get("status") == "not_started":
            return True
        child_id = invocation.get("child_workflow_run_id")
        child = self.read(child_id) if child_id else None
        if child is None:
            return bool(child_id) and invocation.get("stage") == "preparing" and self.root_orphaned()
        link = child.get("parent_link") or {}
        if link.get("workflow_run_id") != run.get("workflow_run_id") or link.get("execution_id") != activation.get("id"):
            return False
        return self.run_settled(child)

    def run_settled(self, run: dict[str, Any]) -> bool:
        run_id = run.get("workflow_run_id")
        if run_id in self.visited or len(self.visited) >= TREE_RUN_LIMIT:
            return False
        self.visited.add(run_id)
        if run.get("status") not in wait_policy.WORKFLOW_TERMINAL or tasks_settling(run):
            return False
        return all(self.activation_settled(run, activation) for activation in run.get("activations", []))


def tree_settlement(store: Any, run: dict[str, Any], *, now: float, grace_seconds: float = 600,
                    observed_at: float | None = None) -> str:
    """Worker-side check that a terminal root's invocation children have settled.

    Returns "settled", or "pending" while the point (`observed_at`, else the run's `updated_at`)
    is younger than `grace_seconds`, else "unconfirmed". At every depth an invocation counts as
    settled when it is `not_started`, when its child exists, links back and is settled
    recursively, or when its child is missing at stage `preparing` and the ROOT is terminal with
    its supervisor confirmed gone (the root supervisor owns the whole tree). Reads share one
    metadata budget and byte limit; bounded to 64 runs. A budget or size failure, or anything
    unreadable, is unsettled.

    A failed point whose root was recovered while its children were still unwinding reads as
    pending for the full grace period and then as unconfirmed. Stage 3 may deliver it
    immediately as unconfirmed once the current key no longer matches the point.
    """
    try:
        check = _TreeCheck(store, run)
        if all(check.activation_settled(run, activation) for activation in run.get("activations", [])):
            return "settled"
    except Exception:
        log.warning("Unable to check workflow tree settlement", exc_info=True)
    start = observed_at if isinstance(observed_at, (int, float)) else run.get("updated_at")
    if isinstance(start, (int, float)) and now - start < grace_seconds:
        return "pending"
    return "unconfirmed"


def phase(run: dict[str, Any]) -> str:
    return wait_policy.workflow_phase(run.get("status"), tasks_settling(run))


def _source_id(source: Any, default: Any) -> Any:
    return source.get("workflow_run_id", default) if isinstance(source, dict) else default


def point_key(run: dict[str, Any], event: str | None = None) -> str | None:
    """The identity of the notification point the run is at right now, or None.

    Pure: a function of the record alone, so a worker can recompute the *current* key later.
    `event` is the mutation that produced the record; a caller pause never notifies, whether the
    record already says so (`pause_origin`) or the mutation is the pause itself.
    """
    if not is_root(run) or run.get("kind") == "builder":
        return None
    current = phase(run)
    status = run.get("status")
    if current == "terminal":
        if status == "failed":
            since = run.get("failed_since_sequence")
            if since is None:
                # Legacy run, failed before the transition was recorded.
                since = run.get("attention_checkpoint", run.get("sequence", 0))
            return f"terminal:failed:{since}"
        return f"terminal:{status}"
    if current != "attention":
        return None
    root_id = run.get("workflow_run_id")
    if status == "needs_input":
        return f"attention:needs_input:{run.get('input_decision_id')}:{_source_id(run.get('input_source'), root_id)}"
    if status == "paused" and (run.get("pause_origin") == "caller" or event == "control:pause"):
        return None
    source = run.get("attention_source")
    source_checkpoint = source.get("attention_checkpoint") if isinstance(source, dict) else None
    return f"attention:{status}:{run.get('attention_checkpoint')}:{_source_id(source, root_id)}:{source_checkpoint}"


def event_for(key: str) -> str:
    return SETTLED if key.startswith("terminal:") else ATTENTION


def _text(value: Any, limit: int) -> tuple[str, bool]:
    text = value if isinstance(value, str) else ("" if value is None else str(value))
    return text[:limit], len(text) > limit


def _project_source(source: Any) -> dict[str, Any] | None:
    if not isinstance(source, dict):
        return None
    projected: dict[str, Any] = {}
    if "workflow_run_id" in source:
        projected["workflow_run_id"] = str(source["workflow_run_id"])[:100]
    if "workflow_name" in source:
        projected["workflow_name"] = str(source["workflow_name"])[:200]
    if isinstance(source.get("path"), list):
        projected["path"] = [str(item)[:100] for item in source["path"]][:16]
    if isinstance(source.get("attention_checkpoint"), int):
        projected["attention_checkpoint"] = source["attention_checkpoint"]
    return projected or None


def _disposition(run: dict[str, Any]) -> dict[str, Any] | None:
    value = run.get("checklist_disposition")
    if not isinstance(value, dict):
        return None
    projected = {key: str(value[key])[:100] for key in ("status", "execution_id", "decision_id") if key in value}
    projected["reason"] = str(value.get("reason", ""))[:256]
    return projected


def build_snapshot(run: dict[str, Any], scale: int = 1) -> dict[str, Any]:
    """The bounded run-derived part of a notification payload; `scale` shrinks the free text."""
    definition = run.get("definition") if isinstance(run.get("definition"), dict) else {}
    snapshot: dict[str, Any] = {
        "status": run.get("status"),
        "name": run.get("name"),
        "kind": run.get("kind"),
        "goal_revision": run.get("goal_revision"),
        "definition": {
            "workflow_id": definition.get("workflow_id"),
            "revision": run.get("revision"),
            "definition_hash": run.get("definition_hash"),
        },
        "attention": None,
        "result": None,
    }
    if phase(run) == "attention":
        question, question_truncated = _text(run.get("input_question"), QUESTION_LIMIT // scale)
        reason, _ = _text(run.get("attention_reason") or run.get("reason"), REASON_LIMIT // scale)
        snapshot["attention"] = {
            "kind": run.get("status"),
            "input_decision_id": run.get("input_decision_id"),
            "question": question,
            "question_truncated": question_truncated,
            "reason": reason,
            "source": _project_source(run.get("input_source") or run.get("attention_source")),
        }
    else:
        summary, _ = _text(run.get("summary"), SUMMARY_LIMIT // scale)
        failure, _ = _text(run.get("failure_reason"), FAILURE_LIMIT // scale)
        snapshot["result"] = {
            "summary": summary,
            "failure_reason": failure,
            "checklist_disposition": _disposition(run),
        }
    return snapshot


def bounded_snapshot(run: dict[str, Any]) -> dict[str, Any]:
    """`build_snapshot` shrunk until it serializes within SNAPSHOT_BYTE_LIMIT, else ValueError."""
    for scale in (1, 2, 4, 8, 16):
        snapshot = build_snapshot(run, scale)
        if len(json.dumps(snapshot, ensure_ascii=False).encode()) <= SNAPSHOT_BYTE_LIMIT:
            return snapshot
    raise ValueError("notification snapshot exceeds its byte bound")


def update_markers(run: dict[str, Any], previous_status: Any, event: str, previous_checkpoint: Any = None) -> None:
    """Maintain `failed_since_sequence` and `pause_origin`; call after the sequence is advanced.

    A run that was already failed with no recorded transition (legacy) is backfilled once from its
    pre-mutation checkpoint, else its pre-mutation sequence, and marked `failed_since_source`.
    """
    status = run.get("status")
    if status == "failed":
        if previous_status != "failed":
            run["failed_since_sequence"] = run.get("sequence", 0)
            run.pop("failed_since_source", None)
        elif "failed_since_sequence" not in run:
            run["failed_since_sequence"] = previous_checkpoint if isinstance(previous_checkpoint, int) else run.get("sequence", 1) - 1
            run["failed_since_source"] = "legacy"
    else:
        run.pop("failed_since_sequence", None)
        run.pop("failed_since_source", None)
    if status == "paused":
        if event == "control:pause":
            run["pause_origin"] = "caller"
    else:
        run.pop("pause_origin", None)


def _sequence(point: Any) -> int | None:
    if isinstance(point, dict) and type(point.get("sequence")) is int:
        return point["sequence"]
    return None


def _latest_key(run: dict[str, Any]) -> str | None:
    latest = None
    for name in ("notification_points", "terminal_notification_points"):
        points = run.get(name)
        if not isinstance(points, list):
            continue
        for point in reversed(points):
            sequence = _sequence(point)
            if sequence is not None:
                if latest is None or sequence >= _sequence(latest):
                    latest = point
                break
    return latest.get("key") if latest else None


def _append(run: dict[str, Any], name: str, limit: int, evicted_type: str, point: dict[str, Any]) -> None:
    points = run.get(name)
    if not isinstance(points, list):
        points = run[name] = []
    points.append(point)
    while len(points) > limit:
        dropped = points.pop(0)
        sequence = _sequence(dropped)
        marks = run.get("notification_evicted")
        if not isinstance(marks, dict):
            marks = run["notification_evicted"] = {}
        if sequence is not None:
            previous = marks.get(evicted_type)
            marks[evicted_type] = max(previous, sequence) if type(previous) is int else sequence


def _snapshot_bytes(point: Any) -> int:
    snapshot = point.get("snapshot") if isinstance(point, dict) else None
    return len(json.dumps(snapshot, ensure_ascii=False).encode()) if isinstance(snapshot, dict) else 0


def _enforce_total_bound(run: dict[str, Any], fresh: dict[str, Any] | None = None) -> None:
    """Null the oldest snapshot bodies past the per-run cap, never the point just appended.

    Oldest first by sequence, attention bodies before terminal ones at equal sequence. Identities
    are never dropped for size: a nulled point keeps its key and sequence and is marked
    `snapshot_evicted`.
    """
    ordered = [
        (_sequence(point), 0 if name == "notification_points" else 1, point)
        for name in ("notification_points", "terminal_notification_points")
        for point in (run.get(name) if isinstance(run.get(name), list) else [])
        if isinstance(point, dict) and _sequence(point) is not None
    ]
    ordered.sort(key=lambda item: (item[0], item[1]))
    total = sum(_snapshot_bytes(point) for _, _, point in ordered)
    for _, _, point in ordered:
        if total <= SNAPSHOT_TOTAL_BYTE_LIMIT:
            return
        if point is fresh:
            continue
        size = _snapshot_bytes(point)
        if size:
            point["snapshot"] = None
            point["snapshot_evicted"] = True
            total -= size


def record_point(run: dict[str, Any], event: str) -> None:
    """Append this mutation's notification point to a root run, in two guarded steps.

    The identity (pure field reads) goes in first; the snapshot is attached afterwards, and a
    snapshot failure leaves the identity with `snapshot: null, snapshot_error: true`. If even the
    identity step fails the sequence is recorded in `notification_point_error`. Never raises, and
    never leaves an appended point with neither a snapshot nor `snapshot_error`.
    """
    point: dict[str, Any] | None = None
    try:
        key = point_key(run, event)
        if key is None or key == _latest_key(run):
            return
        kind = event_for(key)
        snapshot: dict[str, Any] | None = None
        snapshot_error = False
        try:
            snapshot = bounded_snapshot(run)
        except Exception:
            log.warning("Unable to build notification snapshot", exc_info=True)
            snapshot_error = True
        point = {
            "key": key,
            "event": kind,
            "status": run.get("status"),
            "sequence": run.get("sequence", 0),
            "observed_at": run.get("updated_at"),
            "snapshot": snapshot,
        }
        if snapshot_error:
            point["snapshot_error"] = True
        if kind == SETTLED and has_started_invocation(run):
            point["requires_tree_check"] = True
        if kind == SETTLED:
            if run.get("failed_since_source") == "legacy" or (
                run.get("status") == "failed" and "failed_since_sequence" not in run
            ):
                point["snapshot_source"] = "legacy_state"
            _append(run, "terminal_notification_points", TERMINAL_POINT_LIMIT, "terminal", point)
        else:
            _append(run, "notification_points", ATTENTION_POINT_LIMIT, "attention", point)
        _enforce_total_bound(run, point)
    except Exception:
        log.warning("Unable to record notification point", exc_info=True)
        try:
            run["notification_point_error"] = run.get("sequence", 0)
            if isinstance(point, dict) and "snapshot" not in point:
                point["snapshot"] = None
                point["snapshot_error"] = True
        except Exception:
            pass
