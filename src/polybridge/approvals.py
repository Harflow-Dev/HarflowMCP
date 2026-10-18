"""Bounded durable permission decisions, independent of task/catalog locks.

The ledger is an audit record, not a reusable permission grant. No lock is held
across an await or task/workflow persistence. Only the owning stdin pump sends.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .store import validate_task_id

MAX_REQUEST_BYTES = 64 * 1024
MAX_PENDING = 16
MAX_RECORDS = 128
MAX_LEDGER_BYTES = 2 * 1024 * 1024
ADMISSION_BYTES = MAX_LEDGER_BYTES - 256 * 1024
REQUEST_SECONDS = 1800
ACTIVE = {"pending", "reserved"}


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")


def digest(action: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(action)).hexdigest()


def _path(directory: Path, task_id: str) -> Path:
    return directory / (validate_task_id(task_id) + ".approvals.json")


def _read(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "requests": []}
    if path.stat().st_size > MAX_LEDGER_BYTES:
        raise ValueError("Approval ledger exceeds its byte bound")
    try:
        data = json.loads(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Approval ledger is unreadable") from exc
    if not isinstance(data, dict) or data.get("version") != 1 or not isinstance(data.get("requests"), list) or len(data["requests"]) > MAX_RECORDS:
        raise ValueError("Invalid approval ledger")
    for row in data["requests"]:
        if not isinstance(row, dict) or not isinstance(row.get("action"), dict) or row.get("action_digest") != digest(row["action"]):
            raise ValueError("Invalid approval action snapshot")
    return data


def _write(path: Path, data: dict[str, Any]) -> None:
    raw = canonical(data)
    if len(raw) > MAX_LEDGER_BYTES:
        raise ValueError("Approval ledger is full")
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@contextmanager
def _locked(directory: Path, task_id: str):
    path = _path(directory, task_id)
    fd = os.open(str(path) + ".lock", os.O_CREAT | os.O_RDWR, 0o600)
    try:
        # Bounded lock contention; a stuck writer cannot stall the event loop forever.
        until = time.monotonic() + 2
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= until:
                    raise ValueError("Approval ledger is busy; retry the read or decision")
                time.sleep(0.01)
        data = _read(path)
        yield data
        _write(path, data)
    finally:
        os.close(fd)


def metadata(row: dict[str, Any]) -> dict[str, Any]:
    return {k: copy.deepcopy(v) for k, v in row.items() if k not in {"action", "bridge_owner", "process_identity"}}


def rows(directory: Path, task_id: str) -> list[dict[str, Any]]:
    # Atomic replacement makes unlocked readers see a complete generation.
    return copy.deepcopy(_read(_path(directory, task_id))["requests"])


def summary(directory: Path, task_id: str) -> dict[str, Any]:
    try:
        records = rows(directory, task_id)
        pending = [r for r in records if r["disposition"] == "pending" and r["delivery"] in ACTIVE]
        uncertain = [r for r in records if r["delivery"] in {"sending", "delivery_unknown"} and r["execution_outcome"] == "unknown"]
        result = {"pending_count": 0, "delivery_uncertain_count": 0, "interaction_state": None, "requests": [], "uncertainty_requests": []}
        if not pending and not uncertain:
            return result
        from . import identity, store
        record = store.read(directory, task_id, include_prompt=False, metadata_byte_limit=256 * 1024)
        if record is None:
            raise ValueError("Approval task record unavailable")
        # PID loss and inferred failure are not observed execution settlement.
        settled = record.exit_code is not None and record.output_drained is True
        if settled:
            result["invalidated_count"] = len(pending)
            return result
        if uncertain:
            result.update(delivery_uncertain_count=len(uncertain), uncertainty_requests=[metadata(r) for r in uncertain], interaction_state="approval_state_uncertain")
        if pending:
            process_state = identity.identity_check(pending[0]["process_identity"])
            owner_state = identity.identity_check(pending[0]["bridge_owner"])
            if record.status != "running" or process_state != "alive" or owner_state != "alive":
                result.update(interaction_state="approval_state_uncertain", error="Approval pipe ownership is lost or uncertain; prior decisions cannot be replayed")
            else:
                result.update(pending_count=len(pending), requests=[metadata(r) for r in pending])
                if not uncertain:
                    result["interaction_state"] = "waiting_for_approval"
        return result
    except (OSError, ValueError, KeyError):
        return {"pending_count": 0, "delivery_uncertain_count": 0, "interaction_state": "approval_state_uncertain", "requests": [], "uncertainty_requests": [], "error": "Approval ledger unavailable; execution must be reconciled"}


def admit(directory: Path, task: Any, decoded: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    if len(canonical(decoded)) > MAX_REQUEST_BYTES:
        raise ValueError("Approval request exceeds 64 KiB")
    now = time.time()
    identity = {"pid": task.proc.pid, "start_time": task.start_time, "markers": list(task.markers)}
    if not task.start_time or not task.owner:
        raise ValueError("Approval process identity is not verified")
    row = {"request_id": uuid.uuid4().hex, "task_id": task.task_id, **copy.deepcopy(decoded), "tool_name": decoded["action"]["tool_name"], "action_digest": digest(decoded["action"]), "owner_revision": 1, "generation": task.approval_generation, "session_id": task.session_id, "bridge_owner": copy.deepcopy(task.owner), "process_identity": identity, "created_at": now, "deadline": min(now + REQUEST_SECONDS, context.get("hard_deadline") or getattr(task, "approval_hard_deadline", None) or now + REQUEST_SECONDS), "cwd": str(task.repo_path), "freedom": task.freedom, "scope": "exact_action_once", "disposition": "pending", "decision": None, "effective_decision": None, "delivered_decision": None, "expiry_rejection": False, "delivery": "pending", "execution_outcome": "unknown", **context}
    with _locked(directory, task.task_id) as data:
        records = data["requests"]
        if any(r["native_request_id"] == row["native_request_id"] or (row["tool_use_id"] != "unsupported" and r["tool_use_id"] == row["tool_use_id"]) for r in records):
            raise ValueError("Repeated native permission request identity")
        if len(records) >= MAX_RECORDS or sum(r["disposition"] == "pending" and r["delivery"] in ACTIVE for r in records) >= MAX_PENDING:
            raise ValueError("Approval request count bound exceeded")
        if len(canonical({**data, "requests": [*records, row]})) > ADMISSION_BYTES:
            raise ValueError("Approval admission storage bound exceeded")
        records.append(row)
    return copy.deepcopy(row)


def get(directory: Path, task_id: str, request_id: str) -> dict[str, Any]:
    for row in rows(directory, task_id):
        if row["request_id"] == request_id:
            return {**metadata(row), "action": row["action"], "complete": True}
    raise ValueError("Approval request not found")


def reserve(directory: Path, task_id: str, request_id: str, action_digest: str, owner_revision: int, decision: str, generation: str) -> dict[str, Any]:
    if decision not in {"approve", "reject"} or type(owner_revision) is not int:
        raise ValueError("A decision must be approve or reject with an integer owner revision")
    with _locked(directory, task_id) as data:
        row = next((r for r in data["requests"] if r["request_id"] == request_id), None)
        if row is None:
            raise ValueError("Approval request not found")
        if row["action_digest"] != action_digest or row["owner_revision"] != owner_revision or row["generation"] != generation:
            raise ValueError("Stale approval action, revision or owner generation")
        if row["deadline"] <= time.time() or row["disposition"] != "pending" or row["delivery"] != "pending":
            raise ValueError("Approval request is expired, settled or already answered")
        row.update(decision=decision, delivery="reserved", owner_revision=owner_revision + 1)
        result = copy.deepcopy(row)
    return result


def update(directory: Path, task_id: str, request_id: str, generation: str, expected_delivery: str | None = None, **changes: Any) -> dict[str, Any]:
    with _locked(directory, task_id) as data:
        row = next((r for r in data["requests"] if r["request_id"] == request_id), None)
        if row is None or row["generation"] != generation or (expected_delivery is not None and row["delivery"] != expected_delivery):
            raise ValueError("Stale approval delivery state")
        if changes.get("delivery") == "sending":
            if row["disposition"] != "pending" or row.get("execution_outcome") != "unknown":
                raise ValueError("Approval action already has an observed outcome")
            expired = row["deadline"] <= time.time()
            changes["effective_decision"] = "reject" if expired else row.get("effective_decision") or row["decision"]
            changes["expiry_rejection"] = row.get("expiry_rejection", False) or expired
        row.update(changes)
        row["owner_revision"] += 1
        result = copy.deepcopy(row)
    return result


def invalidate(directory: Path, task_id: str, disposition: str, generation: str | None = None) -> None:
    with _locked(directory, task_id) as data:
        for row in data["requests"]:
            if (generation is None or row["generation"] == generation) and (row["disposition"] == "pending" or row["delivery"] in {"reserved", "sending"}):
                row.update(disposition=disposition, delivery="delivery_unknown" if row["delivery"] in {"sending", "sent"} else "failed", owner_revision=row["owner_revision"] + 1)


def observe_outcome(directory: Path, task_id: str, request_id: str, generation: str, outcome: str) -> None:
    """Merge harness evidence without replacing a concurrent local delivery state."""
    with _locked(directory, task_id) as data:
        row = next((r for r in data["requests"] if r["request_id"] == request_id), None)
        if row is None or row["generation"] != generation:
            raise ValueError("Stale approval outcome")
        row["execution_outcome"] = outcome
        if row.get("expiry_rejection"):
            row["disposition"] = "expired"
        elif row["disposition"] == "pending":
            row["disposition"] = "decided" if row.get("decision") else "invalidated"
        # A send reservation can already have written bytes. Its owner must still
        # persist local delivery independently of this observed tool outcome.
        if row["delivery"] in {"pending", "reserved"}:
            row["delivery"] = "failed"
        row["owner_revision"] += 1


def finish_delivery(directory: Path, task_id: str, request_id: str, generation: str, written: bool, effective_decision: str, expiry_rejection: bool = False) -> None:
    """Merge a local pipe write with independently observed harness evidence."""
    with _locked(directory, task_id) as data:
        row = next((r for r in data["requests"] if r["request_id"] == request_id), None)
        if row is None or row["generation"] != generation:
            raise ValueError("Stale approval delivery owner")
        # Cancellation/restart may already have made delivery uncertain. Never
        # revive its decision eligibility or erase a correlated tool outcome.
        if row["delivery"] == "sending":
            row["delivery"] = "sent" if written else "delivery_unknown"
        row["effective_decision"] = effective_decision
        row["delivered_decision"] = effective_decision if written else None
        row["expiry_rejection"] = row.get("expiry_rejection", False) or expiry_rejection
        if row["expiry_rejection"]:
            row["disposition"] = "expired"
        elif row["disposition"] == "pending":
            row["disposition"] = "decided"
        row["owner_revision"] += 1


def observe_cancellation(directory: Path, task_id: str, request_id: str, generation: str) -> None:
    """Close harness decision eligibility without guessing whether reserved bytes arrived."""
    with _locked(directory, task_id) as data:
        row = next((r for r in data["requests"] if r["request_id"] == request_id), None)
        if row is None or row["generation"] != generation:
            raise ValueError("Stale approval cancellation")
        row["disposition"] = "invalidated"
        row["delivery"] = "delivery_unknown" if row["delivery"] in {"sending", "sent", "delivery_unknown"} else "failed"
        row["owner_revision"] += 1
