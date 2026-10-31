"""Task-bound phase capacity and host cleanup on the shared resource ledger.

Lock order: session/launch/record, phase lifecycle, catalog, resource ledger leaf.
Receipts precede admission and every release transition. Uncertain external effects
remain blockers until explicit human inspection; cleanup is never replayed.
"""
from __future__ import annotations

from contextlib import contextmanager
import copy
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import threading
import time
import uuid

from . import identity, store
from .resources import (ResourceLedger, InvalidResourceRequest, ResourceLedgerCorrupt,
                        ResourceLedgerRepairing, ResourceError, Owner, _fsync_directory, _atomic_write, _empty_document,
                        _now, validate_names, validate_reservation_id, MAX_ENTRIES)
from .state_access import task_operation
from .state_layout import resolve_layout
from .state_ownership import state_session, exclusive_session, _read_marker, MARKER_NAME
from .wire_contracts import PRODUCT, PRODUCT_VERSION
from .state_compatibility import persisted_compatible

FORMAT = "harflow.phase-lease"
BOOTSTRAP = ".phase-slots-bootstrap.json"
MAX_BYTES = 65536
FINAL = frozenset({"released", "expired", "resolved"})
CLEANUP = frozenset({"cleanup_pending", "cleanup_running", "cleanup_failed", "cleanup_uncertain"})
_local = threading.local()
log = logging.getLogger(__name__)


class SlotRefused(InvalidResourceRequest):
    code = "slot_refused"


class SlotNotEnabled(SlotRefused):
    code = "phase_slots_not_enabled"


class SlotSourceChanged(SlotRefused):
    code = "slot_source_changed"


class SlotLifecycleBusy(SlotRefused):
    code = "slot_lifecycle_busy"


def _directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise SlotRefused("slot directory must be an owned directory without group/world writes")


def _read_private(path: Path, *, missing=False, observed_path=False):
    """Retry only a positively replaced source, before interpreting its contents.

    This is a bounded file acquisition retry, never a transaction, reservation or
    action retry. Malformed or otherwise unqualified evidence is not retried.
    """
    for attempt in range(3):
        try:
            return _read_private_once(path, missing=missing, observed_path=observed_path)
        except SlotSourceChanged:
            if attempt == 2:
                raise


def _read_private_once(path: Path, *, missing=False, observed_path=False):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        if observed_path:
            raise SlotSourceChanged("observed slot evidence disappeared before opening") from None
        if missing:
            return None
        raise SlotRefused("required slot evidence is missing") from None
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise SlotRefused("slot evidence must be an owned 0600 regular file")
        source = (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size)
        payload = handle.read(MAX_BYTES + 1)
        after = os.fstat(handle.fileno())
        try:
            current = path.lstat()
        except FileNotFoundError:
            raise SlotSourceChanged("slot evidence disappeared during read") from None
        if any((item.st_dev, item.st_ino, item.st_mtime_ns, item.st_size) != source for item in (after, current)):
            raise SlotSourceChanged("slot evidence changed during read")
    if len(payload) > MAX_BYTES:
        raise SlotRefused("slot evidence exceeds its size bound")
    try:
        value = json.loads(payload)
    except (ValueError, UnicodeError, RecursionError):
        raise SlotRefused("slot evidence is not qualified JSON") from None
    if not isinstance(value, dict):
        raise SlotRefused("slot evidence is not an object")
    return value


@contextmanager
def lifecycle_lock(log_dir: Path, task_id: str):
    """Reentrant only within a synchronous thread and current process; no awaits."""
    store.validate_task_id(task_id)
    key = (os.getpid(), str(log_dir), task_id)
    held = getattr(_local, "held", None)
    if held is None:
        held = _local.held = set()
    if key in held:
        yield
        return
    directory = log_dir.parent / "phase-locks"
    _directory(directory)
    fd = os.open(directory / (task_id + ".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        deadline = time.monotonic() + 2
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise SlotLifecycleBusy("task phase lifecycle is busy; retry") from None
                time.sleep(.01)
        held.add(key)
        try:
            yield
        finally:
            held.remove(key)
    finally:
        os.close(fd)


def _enabled(root: Path):
    if (_read_marker(root) or {}).get("layout_version") != 2:
        raise SlotNotEnabled("phase slots are disabled; a human must quiesce all owners and run harflow-ctl slot enable")


def validate_configuration(value):
    if (type(value) is not dict or set(value) != {"epoch", "names"}
            or type(value["epoch"]) is not int or value["epoch"] < 0
            or type(value["names"]) is not dict or len(value["names"]) > 256):
        raise ValueError("invalid slot configuration")
    for name, policy in value["names"].items():
        validate_names([name])
        if type(policy) is not dict or set(policy) - {"capacity", "cleanup"}:
            raise ValueError("invalid slot policy")
        if type(policy.get("capacity")) is not int or not 1 <= policy["capacity"] <= 4096:
            raise ValueError("slot capacity must be 1..4096")
        hook = policy.get("cleanup")
        if hook is not None:
            if policy["capacity"] != 1:
                raise ValueError("cleanup requires capacity one")
            if type(hook) is not dict or set(hook) != {"argv", "timeout_seconds", "executable"}:
                raise ValueError("invalid cleanup policy")
            _validate_argv(hook["argv"])
            if type(hook["timeout_seconds"]) is not int or not 1 <= hook["timeout_seconds"] <= 300:
                raise ValueError("cleanup timeout must be 1..300 seconds")
            if type(hook["executable"]) is not dict or set(hook["executable"]) != {"device", "inode", "size", "mtime_ns", "sha256"}:
                raise ValueError("invalid executable identity")
    return value


def _validate_argv(argv):
    if (type(argv) is not list or not 1 <= len(argv) <= 32
            or any(type(arg) is not str or "\0" in arg or len(arg) > 4096 for arg in argv)
            or sum(len(arg) for arg in argv) > 16384 or not os.path.isabs(argv[0])):
        raise SlotRefused("cleanup requires 1..32 explicit argv tokens and an absolute executable; shell interpolation is unsupported")


def _executable_opened(handle):
    info = os.fstat(handle.fileno())
    if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o111 or info.st_size > 256 * 1024 * 1024:
        raise SlotRefused("cleanup executable is not a bounded executable regular file")
    digest = hashlib.file_digest(handle, "sha256").hexdigest()
    after = os.fstat(handle.fileno())
    if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
        raise SlotRefused("cleanup executable changed while qualifying")
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "sha256": digest}


def _executable(path):
    # Follow configured executable symlinks, but bind the actual opened inode.
    with open(path, "rb") as handle:
        return _executable_opened(handle)


def initialize_document(root):
    bootstrap = _read_private(root / BOOTSTRAP)
    expected = {"product": PRODUCT, "format": "harflow.phase-bootstrap", "v": 1,
                "product_version": PRODUCT_VERSION, "root": str(root), "state": "prepared"}
    if bootstrap != expected:
        raise SlotRefused("phase bootstrap is not qualified; retain state for human inspection")
    return _empty_document() | {"v": 2, "slot_configuration": {"epoch": 0, "names": {}}, "slot_events": []}


def enable(root):
    layout = resolve_layout(root=root)
    with exclusive_session(layout, timeout=0.25):
        marker = _read_marker(layout.root)
        if marker["layout_version"] == 2:
            ledger = ResourceLedger(root)
            bootstrap = _read_private(layout.root / BOOTSTRAP)
            if not ledger.path.exists() and bootstrap.get("state") != "prepared":
                raise SlotRefused("initialized layout 2 ledger is missing; retain evidence for explicit repair")
            from .state_recovery import _check_owners
            _check_owners(layout.root)
            ledger._bootstrap_initialization = bootstrap.get("state") == "prepared"
            ledger._transact(lambda tx: setattr(tx, "dirty", True))
            _atomic_write(layout.root / BOOTSTRAP, bootstrap | {"state": "initialized"})
            return {"enabled": True, "layout_version": 2}
        from .state_recovery import _check_owners
        _check_owners(layout.root)
        ledger = ResourceLedger(root)
        if ledger._read_only():
            raise SlotRefused("enable requires no legacy reservations")
        for name in ("phase-leases", "phase-locks"):
            path = layout.root / name
            if path.is_symlink() or path.exists() and (not path.is_dir() or any(path.iterdir())):
                raise SlotRefused("preexisting phase evidence prevents enable")
        for name in ("slot-service.json", ".slot-service.lock"):
            path = layout.root / "resources" / name
            if path.exists() or path.is_symlink():
                raise SlotRefused("preexisting supervisor evidence prevents enable")
        expected = {"product": PRODUCT, "format": "harflow.phase-bootstrap", "v": 1,
                    "product_version": PRODUCT_VERSION, "root": str(layout.root), "state": "prepared"}
        prepared = _read_private(layout.root / BOOTSTRAP, missing=True)
        if prepared is not None and prepared != expected:
            raise SlotRefused("unqualified prepared enable prevents promotion")
        _atomic_write(layout.root / BOOTSTRAP, expected)
        _atomic_write(layout.root / MARKER_NAME, marker | {"layout_version": 2})
        ledger._bootstrap_initialization = True
        ledger._transact(lambda tx: setattr(tx, "dirty", True))
        _atomic_write(layout.root / BOOTSTRAP, expected | {"state": "initialized"})
    return {"enabled": True, "layout_version": 2}


def _chain(log_dir, task_id):
    from .task_supervision import read_launch
    result = []
    while task_id:
        store.validate_task_id(task_id)
        if task_id in result or len(result) >= 64:
            raise SlotRefused("task ancestry is cyclic or exceeds qualification bounds")
        result.append(task_id)
        record = store.read(log_dir, task_id, include_prompt=False)
        receipt = read_launch(log_dir, task_id) if record is None else None
        if record is None and receipt is None:
            raise SlotRefused("task ancestry evidence is missing")
        task_id = record.spawned_by if record is not None else receipt.get("spawned_by")
    return result


def owner_lineage(root, owner):
    from .workflows import WorkflowStore
    from .task_supervision import read_launch
    from .workflow_receipt_io import read
    initial = ("task", owner.task_id) if owner.kind == "task" else ("workflow", owner.workflow_run_id)
    queue = [(*initial, frozenset())]
    result = []
    seen = set()
    storage = None
    if owner.kind == "workflow_attempt":
        if not owner.activation_id:
            raise SlotRefused("workflow attempt provenance is missing")
        result.append("attempt:" + owner.workflow_run_id + ":" + owner.activation_id)
    while queue:
        kind, identifier, path_keys = queue.pop(0)
        if not identifier:
            raise SlotRefused("resource owner identity is incomplete")
        key = kind + ":" + identifier
        if key in path_keys:
            raise SlotRefused("scope ancestry is cyclic")
        if key in seen:
            continue
        next_path = path_keys | {key}
        if len(seen) >= 128:
            raise SlotRefused("scope ancestry exceeds qualification bounds")
        seen.add(key)
        result.append(key)
        if kind == "task":
            store.validate_task_id(identifier)
            record = store.read(root / "tasks", identifier, include_prompt=False)
            receipt = read_launch(root / "tasks", identifier) if record is None else None
            if record is None and receipt is None:
                raise SlotRefused("task ancestry evidence is missing")
            parent = record.spawned_by if record is not None else receipt.get("spawned_by")
            if parent:
                queue.append(("task", parent, next_path))
            path = root / "workflow-owners" / (identifier + ".json")
            if path.exists() or path.is_symlink():
                workflow_owner, _ = read(path)
                rid = workflow_owner.get("workflow_run_id")
                aid = workflow_owner.get("activation_id")
                if not rid:
                    raise SlotRefused("workflow scope provenance is missing")
                if aid:
                    result.append("attempt:" + rid + ":" + aid)
                queue.append(("workflow", rid, next_path))
        else:
            if storage is None:
                storage = WorkflowStore(root)
            run = storage.get_run(identifier)
            if not isinstance(run, dict):
                raise SlotRefused("workflow scope ancestry evidence is missing")
            parent = (run.get("parent_link") or {}).get("workflow_run_id")
            if parent:
                queue.append(("workflow", parent, next_path))
            caller = run.get("caller_record")
            if caller is not None:
                if not isinstance(caller, dict) or not caller.get("task_id"):
                    raise SlotRefused("workflow caller ancestry is unqualified")
                current = store.read(root / "tasks", caller["task_id"], include_prompt=False)
                if current is None or current.started_at != caller.get("started_at"):
                    raise SlotRefused("workflow caller task generation is unavailable")
                queue.append(("task", caller["task_id"], next_path))
    return list(dict.fromkeys(result))


def _task_lineage(log_dir, record):
    return owner_lineage(log_dir.parent, Owner("task", task_id=record.task_id))


def check_conflicts(tx, owner, chain, *, phase):
    own = chain[0]
    for entry in tx.entries:
        if not phase and entry.get("purpose") != "phase":
            continue
        other = entry.get("scope_chain")
        if not other:
            raise SlotRefused("unqualified reservation ancestry prevents phase interaction")
        if own in other or other[0] in chain:
            raise SlotRefused("named reservations and phase leases cannot stack across a task/workflow lineage")


def _generation(record, launch):
    if not record.start_time or not record.pid or not isinstance(record.owner, dict):
        raise SlotRefused("task execution generation is incomplete")
    try:
        _validate_identity(record.owner)
        _validate_identity(identity.task_identity(record.pid, record.start_time, record.markers))
    except ValueError:
        raise SlotRefused("task owner/agent generation is incomplete") from None
    return hashlib.sha256(json.dumps({"task_id": record.task_id, "started_at": record.started_at,
        "pid": record.pid, "start_time": record.start_time, "owner": record.owner,
        "agent_identity": identity.task_identity(record.pid, record.start_time, record.markers),
        "launch_generation": (launch or {}).get("generation"), "spawned_by": record.spawned_by,
        "freedom": record.freedom, "network": record.network, "enforcement": record.enforcement}, sort_keys=True).encode()).hexdigest()


def _authority(log_dir):
    from .lineage import detect_caller_detail
    detail = detect_caller_detail(log_dir)
    if detail.undecidable or detail.caller is None:
        raise SlotRefused("phase mutation requires a verified live task caller: " + (detail.undecidable or "no task caller"))
    return detail.caller.record


def _live(log_dir, previous):
    from .task_supervision import read_launch
    from .task_resources import cancel_requested
    record = store.read(log_dir, previous.task_id, include_prompt=False)
    launch = read_launch(log_dir, previous.task_id)
    if (record is None or record.status in store.TERMINAL_RECORD_STATUSES
            or cancel_requested(log_dir, previous.task_id)
            or (launch or {}).get("state") in ("settled", "not_started")):
        raise SlotRefused("task is settled, cancelling, or no longer owns this execution")
    if _generation(record, launch) != _generation(previous, launch):
        raise SlotRefused("task generation changed")
    return record, launch


def write_receipt(root, raw):
    rid = validate_reservation_id(raw["reservation_id"])
    directory = root / "phase-leases"
    _directory(directory)
    value = {"product": PRODUCT, "format": FORMAT, "v": 1, "product_version": PRODUCT_VERSION, "entry": raw}
    if len(json.dumps(value).encode()) > MAX_BYTES:
        raise SlotRefused("phase receipt exceeds size bound")
    _atomic_write(directory / (rid + ".json"), value)


def _receipts(root):
    paths = _receipt_paths(root)
    rows = []
    for path in paths:
        if not path.name.endswith(".json"):
            continue
        value = _read_private(path, observed_path=True)
        raw = value.get("entry")
        if (value.get("product") != PRODUCT or value.get("format") != FORMAT or type(value.get("v")) is not int or value["v"] != 1
                or not persisted_compatible(value.get("product_version"), FORMAT)
                or not isinstance(raw, dict) or raw.get("purpose") != "phase"
                or path.name != validate_reservation_id(raw.get("reservation_id")) + ".json"):
            raise SlotRefused("unqualified phase receipt prevents recovery")
        validate_names(raw.get("names"))
        store.validate_task_id(raw.get("owner", {}).get("task_id"))
        if not raw.get("execution_generation") or not raw.get("scope_chain"):
            raise SlotRefused("phase receipt execution identity is missing")
        try:
            validate_phase_entry(raw)
        except (ValueError, KeyError, TypeError) as exc:
            raise SlotRefused("phase receipt qualification failed: " + str(exc)) from None
        rows.append(raw)
    return rows



def validate_events(events):
    if type(events) is not list or len(events) > 256:
        raise ValueError("invalid slot event history")
    for event in events:
        if type(event) is not dict or set(event) != {"at", "kind", "lease_id", "task_id", "name"}:
            raise ValueError("invalid slot lifecycle event")
        if type(event["kind"]) is not str or not 1 <= len(event["kind"]) <= 64:
            raise ValueError("invalid slot lifecycle event kind")
        validate_reservation_id(event["lease_id"])
        store.validate_task_id(event["task_id"])
        validate_names([event["name"]])
        if type(event["at"]) is not str or len(event["at"]) > 64 or datetime.fromisoformat(event["at"]).tzinfo is None:
            raise ValueError("invalid slot event timestamp")


def _validate_identity(value):
    if (type(value) is not dict or type(value.get("pid")) is not int or value["pid"] <= 0
            or type(value.get("start_time")) is not str or not 1 <= len(value["start_time"]) <= 128
            or type(value.get("markers")) is not list or len(value["markers"]) > 32
            or any(type(item) is not str or len(item) > 1024 for item in value["markers"])):
        raise ValueError("process generation identity is incomplete")


def validate_phase_entry(raw):
    if raw.get("purpose") != "phase":
        return
    if (raw.get("phase_state") not in {"queued", "held", *CLEANUP, *FINAL}
            or raw.get("owner", {}).get("kind") != "task"
            or type(raw.get("execution_generation")) is not str
            or len(raw["execution_generation"]) != 64
            or type(raw.get("scope_chain")) is not list
            or not 1 <= len(raw["scope_chain"]) <= 256
            or any(type(x) is not str or len(x) > 200 for x in raw["scope_chain"])
            or len(raw.get("names", [])) != 1
            or type(raw.get("agent_identity")) is not dict
            or type(raw.get("owner_identity")) is not dict):
        raise ValueError("phase entry authority/lifecycle is unqualified")
    store.validate_task_id(raw["owner"].get("task_id"))
    validate_names(raw.get("names"))
    validate_reservation_id(raw.get("reservation_id"))
    if type(raw.get("generation")) is not int or raw["generation"] < 1 or raw.get("state") not in {"held", "queued"}:
        raise ValueError("invalid phase generation or capacity state")
    if raw.get("phase_state") in CLEANUP and raw["state"] != "held":
        raise ValueError("cleanup must retain held capacity")
    for name in ("enqueued_at", "deadline_at", "granted_at", "cleanup_started_at", "cleanup_finished_at", "release_requested_at"):
        value = raw.get(name)
        if value is not None:
            if type(value) is not str or len(value) > 64 or datetime.fromisoformat(value).tzinfo is None:
                raise ValueError("phase timestamps must be bounded timezone-aware ISO8601")
    if not raw.get("enqueued_at") or raw["state"] == "held" and not raw.get("granted_at"):
        raise ValueError("phase timestamp evidence is incomplete")
    _validate_identity(raw["agent_identity"])
    _validate_identity(raw["owner_identity"])
    if raw.get("cleanup_runner") is not None:
        _validate_identity(raw["cleanup_runner"])
    evidence = raw.get("cleanup_evidence", [])
    if type(evidence) is not list or len(evidence) > MAX_CLEANUP_EVIDENCE:
        raise ValueError("invalid retained cleanup evidence")
    for observation in evidence:
        _validate_cleanup_observation(observation)
    if raw.get("cleanup_claim") is not None:
        _observations(raw)
    elif any(raw.get(key) is not None for key in ("cleanup_supervisor", "cleanup_runner", "cleanup_runner_tracking")):
        raise ValueError("cleanup progress has no qualified claim")
    for key in ("expiry_acknowledged_at", "untracked_cleanup_acknowledged_at"):
        if raw.get(key) is not None and not _timestamp(raw[key]):
            raise ValueError("invalid phase acknowledgment timestamp")
    if raw.get("diagnostic") is not None and (type(raw["diagnostic"]) is not str or len(raw["diagnostic"]) > 1000):
        raise ValueError("phase diagnostic exceeds bounds")
    authority = raw.get("task_authority")
    if (type(authority) is not dict or set(authority) != {"freedom", "network", "network_access"}
            or authority["freedom"] not in {"read_only", "write_in_repo", "publish", "unrestricted"}
            or authority["network"] is not None and type(authority["network"]) is not bool
            or authority["network_access"] is not None and type(authority["network_access"]) is not str):
        raise ValueError("phase task authority snapshot is incomplete")
    if raw.get("cleanup_policy") is not None:
        if authority["freedom"] != "unrestricted" or authority["network"] is False or authority["network_access"] not in {"enabled", "unrestricted", "not_controlled"}:
            raise ValueError("cleanup phase lacks unrestricted network-compatible authority")
        validate_configuration({"epoch": 0, "names": {raw["names"][0]: {"capacity": 1, "cleanup": raw["cleanup_policy"]}}})


MAX_CLEANUP_EVIDENCE = 16
_PROGRESS_FIELDS = ("cleanup_claim", "cleanup_supervisor", "cleanup_runner", "cleanup_runner_tracking", "cleanup_started_at", "cleanup_finished_at")
_TRACKING = {"unknown", "spawn_pending", "recorded", "not_spawned", "stopped_untracked"}


def _timestamp(value):
    return type(value) is str and len(value) <= 64 and datetime.fromisoformat(value).tzinfo is not None


def _validate_cleanup_observation(value):
    keys = {"execution_generation", *_PROGRESS_FIELDS}
    if type(value) is not dict or set(value) != keys:
        raise ValueError("invalid retained cleanup observation")
    if type(value["execution_generation"]) is not str or len(value["execution_generation"]) != 64:
        raise ValueError("cleanup observation execution is incomplete")
    validate_reservation_id(value["cleanup_claim"])
    validate_reservation_id(value["cleanup_supervisor"])
    if value["cleanup_runner_tracking"] not in _TRACKING:
        raise ValueError("invalid cleanup runner tracking")
    if value["cleanup_runner"] is not None:
        _validate_identity(value["cleanup_runner"])
    if value["cleanup_runner_tracking"] == "recorded" and value["cleanup_runner"] is None:
        raise ValueError("recorded cleanup runner identity is missing")
    for key in ("cleanup_started_at", "cleanup_finished_at"):
        if value[key] is not None and not _timestamp(value[key]):
            raise ValueError("invalid retained cleanup timestamp")


def _observations(raw):
    result = copy.deepcopy(raw.get("cleanup_evidence", []))
    if raw.get("cleanup_claim") is not None:
        observation = {"execution_generation": raw["execution_generation"],
            **{key: copy.deepcopy(raw.get(key)) for key in _PROGRESS_FIELDS}}
        observation["cleanup_runner_tracking"] = raw.get("cleanup_runner_tracking", "unknown")
        _validate_cleanup_observation(observation)
        result.append(observation)
    unique = {json.dumps(item, sort_keys=True, separators=(",", ":")): item for item in result}
    if len(unique) > MAX_CLEANUP_EVIDENCE:
        raise SlotRefused("cleanup evidence exceeds bounds; retain both documents for human inspection")
    return list(unique.values())


def _merge_progress(raw, receipt):
    evidence = _observations(raw) + (_observations(receipt) if receipt else [])
    unique = {json.dumps(item, sort_keys=True, separators=(",", ":")): item for item in evidence}
    if len(unique) > MAX_CLEANUP_EVIDENCE:
        raise SlotRefused("cleanup evidence exceeds bounds; retain both documents for human inspection")
    raw["cleanup_evidence"] = list(unique.values())
    compatible_attempt = receipt and (raw.get("cleanup_claim") is None or all(raw.get(key) == receipt.get(key) for key in ("execution_generation", "cleanup_claim", "cleanup_supervisor")))
    if compatible_attempt:
        for key in _PROGRESS_FIELDS:
            if raw.get(key) is None and receipt.get(key) is not None:
                raw[key] = copy.deepcopy(receipt[key])
    return raw


def _progress(raw):
    # Receipt events precede the ledger generation increment. That intentional
    # generation lag alone is not an interrupted cleanup publication.
    return {**{key: raw.get(key) for key in _PROGRESS_FIELDS},
        "cleanup_evidence": sorted(json.dumps(item, sort_keys=True) for item in _observations(raw))}


def _binding(raw):
    return {key: raw.get(key) for key in ("reservation_id", "execution_generation", "owner", "names", "scope_chain", "cleanup_policy", "task_authority", "owner_identity", "agent_identity", "enqueued_at", "deadline_at", "policy_epoch")}


def _final_compatible(raw, receipt):
    if _binding(raw) != _binding(receipt):
        return False
    later = _observations(receipt)
    for old in _observations(raw):
        matches = [item for item in later if all(item[key] == old[key] for key in ("execution_generation", "cleanup_claim", "cleanup_supervisor"))]
        if not matches or old["cleanup_runner"] is not None and not any(item["cleanup_runner"] == old["cleanup_runner"] for item in matches):
            return False
    return True


def _uncertain(tx, root, raw, diagnostic, receipt=None):
    _merge_progress(raw, receipt)
    raw.update(state="held", phase_state="cleanup_uncertain", diagnostic=diagnostic)
    raw.setdefault("granted_at", raw["enqueued_at"])
    tx.phase_event(raw, "cleanup_uncertain")
    tx.touch(raw)


def _prunable_final(raw):
    return raw.get("phase_state") in {"released", "resolved"} or raw.get("phase_state") == "expired" and raw.get("expiry_acknowledged_at") is not None


def _unlink_receipt(root, expected):
    path = root / "phase-leases" / (expected["reservation_id"] + ".json")
    current = _read_private(path)
    if current.get("entry") != expected:
        raise SlotSourceChanged("phase receipt changed before pruning; retry")
    path.unlink()
    _fsync_directory(path.parent)


def _receipt_paths(root):
    directory = root / "phase-leases"
    if not directory.exists():
        return []
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise SlotRefused("phase receipt directory is unqualified")
    paths = []
    with os.scandir(directory) as entries:
        for item in entries:
            paths.append(directory / item.name)
            if len(paths) > MAX_ENTRIES:
                raise SlotRefused("phase receipt count exceeds bounds; human archival is required")
    return sorted(paths)


def _slot_snapshot(log_dir):
    ledger = ResourceLedger(log_dir.parent)
    descriptor = ledger._lock()
    try:
        rows = copy.deepcopy(ledger._read_entries())
        return ledger, rows, _receipts(log_dir.parent)
    finally:
        ledger._unlock(descriptor)


def _unacknowledged_expiry(raw):
    return raw.get("phase_state") == "expired" and raw.get("expiry_acknowledged_at") is None


def recover_transaction(tx, root):
    receipts = {raw["reservation_id"]: raw for raw in _receipts(root)}
    durable_ids = {raw["reservation_id"] for raw in tx.entries}
    tx.durable_reservation_ids = durable_ids
    # Prune only after absence from the document read from disk is established.
    # A write-ahead final receipt whose removal happens below survives this pass.
    for rid, receipt in list(receipts.items()):
        if rid not in durable_ids and _prunable_final(receipt):
            _unlink_receipt(root, receipt)
            del receipts[rid]
    for raw in list(tx.entries):
        if raw.get("purpose") != "phase":
            continue
        receipt = receipts.get(raw["reservation_id"])
        if receipt and receipt.get("phase_state") in FINAL and _final_compatible(raw, receipt):
            tx.entries.remove(raw)
            tx.dirty = True
        elif (not receipt or _binding(raw) != _binding(receipt)
                or receipt.get("phase_state") != raw.get("phase_state")
                or _progress(raw) != _progress(receipt)):
            _uncertain(tx, root, raw, "receipt/ledger transition is uncertain; human inspection required", receipt)
    known = {raw["reservation_id"] for raw in tx.entries}
    for rid, receipt in receipts.items():
        if rid not in known and receipt.get("phase_state") not in FINAL:
            if len(tx.entries) >= MAX_ENTRIES:
                raise SlotRefused("unfinished phase evidence exceeds ledger capacity; human inspection required")
            raw = copy.deepcopy(receipt)
            _uncertain(tx, root, raw, "unfinished admission/ledger publication; human inspection required")
            tx.entries.append(raw)
            tx.dirty = True


def repair_entries(root):
    rows = []
    for raw in _receipts(root):
        if raw.get("phase_state") in FINAL:
            continue
        raw = copy.deepcopy(raw)
        raw.update(state="held", phase_state="cleanup_uncertain", diagnostic="receipt restored after ledger uncertainty; human inspection required")
        raw.setdefault("granted_at", raw["enqueued_at"])
        write_receipt(root, raw)
        rows.append(raw)
    return rows


def repair_configuration(root, corrupt_path):
    try:
        value = json.loads(corrupt_path.read_bytes())
        validate_configuration(value.get("slot_configuration"))
    except (ValueError, OSError, AttributeError):
        # Keep the closed corrupt ledger rather than assume hook/capacity defaults.
        raise ResourceLedgerRepairing("v2 configuration is corrupt; human validated configuration restoration is required before repair") from None
    return _empty_document() | {"v": 2, "slot_configuration": value["slot_configuration"], "slot_events": []}


@task_operation
def configure(log_dir, name, capacity=1, cleanup_argv=None, cleanup_timeout=60):
    _enabled(log_dir.parent)
    validate_names([name])
    policy = {"capacity": capacity}
    if cleanup_argv is not None:
        _validate_argv(cleanup_argv)
        policy["cleanup"] = {"argv": cleanup_argv, "timeout_seconds": cleanup_timeout,
                             "executable": _executable(cleanup_argv[0])}
    validate_configuration({"epoch": 0, "names": {name: policy}})
    def action(tx):
        configuration = tx.document["slot_configuration"]
        previous = configuration["names"].get(name, {})
        if previous.get("cleanup") != policy.get("cleanup") and any(name in e["names"] for e in tx.entries):
            raise SlotRefused("cleanup policy cannot change while any reservation mentions the name")
        configuration["names"][name] = policy
        configuration["epoch"] += 1
        tx.dirty = True
        tx.grant_pass()
        return copy.deepcopy(configuration)
    return ResourceLedger(log_dir.parent)._transact(action)



@task_operation
def restore_configuration(log_dir, configuration):
    _enabled(log_dir.parent)
    validate_configuration(configuration)
    ledger = ResourceLedger(log_dir.parent)
    descriptor = ledger._lock()
    try:
        try:
            _, repair = ledger._read_document()
        except ResourceLedgerCorrupt:
            repair = True
        if not repair:
            raise SlotRefused("healthy configuration must change through slot configure")
        if ledger.path.exists():
            backup = ledger.directory / ("ledger.configuration-backup-" + uuid.uuid4().hex + ".json")
            os.link(ledger.path, backup, follow_symlinks=False)
        else:
            backup = None
        document = _empty_document() | {"v": 2, "entries": [], "slot_configuration": configuration, "slot_events": [],
            "repair": {"id": uuid.uuid4().hex, "started_at": _now(), "moved_to": str(backup) if backup else None}}
        _atomic_write(ledger.path, document)
        return {"state": "repair_required", "next_step": "run harflow-ctl resources-repair to reconstruct all durable claims"}
    finally:
        ledger._unlock(descriptor)


def service_status(root):
    value = _read_private(root / "resources" / "slot-service.json", missing=True)
    if value is None:
        return {"state": "missing"}
    if (value.get("product") != PRODUCT or value.get("format") != "harflow.slot-service"
            or type(value.get("v")) is not int or value["v"] != 1
            or not persisted_compatible(value.get("product_version"), "harflow.slot-service")
            or type(value.get("generation")) is not str or len(value["generation"]) != 32
            or "stop_requested" in value and type(value["stop_requested"]) is not bool):
        raise SlotRefused("slot supervisor evidence is unqualified")
    try:
        _validate_identity(value.get("identity"))
        int(value["generation"], 16)
    except (ValueError, TypeError):
        raise SlotRefused("slot supervisor generation is unqualified") from None
    return {"state": "stopping" if value.get("stop_requested") else identity.identity_check(value.get("identity")), "generation": value.get("generation")}


@task_operation
def acquire(log_dir, name, wait_seconds=300):
    _enabled(log_dir.parent)
    validate_names([name])
    if type(wait_seconds) is not int or not 0 <= wait_seconds <= 21600:
        raise SlotRefused("wait_seconds must be an integer from 0 to 21600")
    previous = _authority(log_dir)
    from .task_supervision import read_launch
    expected_execution = _generation(previous, read_launch(log_dir, previous.task_id))
    chain = _task_lineage(log_dir, previous)
    # Probes never run under the ledger leaf lock.
    supervisor = service_status(log_dir.parent)
    reconcile(log_dir)
    with lifecycle_lock(log_dir, previous.task_id):
        record, launch = _live(log_dir, previous)
        execution = _generation(record, launch)
        if execution != expected_execution:
            raise SlotRefused("task launch/execution changed during admission")
        ledger = ResourceLedger(log_dir.parent)
        def action(tx):
            for entry in tx.entries:
                if entry.get("purpose") == "phase" and entry["owner"].get("task_id") == record.task_id:
                    if entry.get("execution_generation") != execution or entry["names"] != [name]:
                        raise SlotRefused("a different phase request or execution already owns capacity")
                    tx.grant_pass()
                    return _view(tx, entry)
            expired = [item for item in _receipts(log_dir.parent) if _unacknowledged_expiry(item)
                and item["owner"].get("task_id") == record.task_id and item["execution_generation"] == execution and item["names"] == [name]]
            if expired:
                return _view(tx, max(expired, key=lambda item: (item["enqueued_at"], item["reservation_id"])))
            check_conflicts(tx, {"kind": "task", "task_id": record.task_id}, chain, phase=True)
            policy = tx.document["slot_configuration"]["names"].get(name, {})
            hook = policy.get("cleanup")
            service_receipt = _read_private(log_dir.parent / "resources" / "slot-service.json", missing=True)
            if hook and (not service_receipt or service_receipt.get("stop_requested") or service_receipt.get("generation") != supervisor.get("generation") or record.freedom != "unrestricted" or record.network is False
                    or (record.enforcement or {}).get("network_access") not in ("enabled", "unrestricted", "not_controlled")
                    or supervisor["state"] != "alive"):
                raise SlotRefused("hook slots require unrestricted persisted network-compatible authority and a live host supervisor")
            if len(tx.entries) >= MAX_ENTRIES:
                raise SlotRefused("resource ledger is full")
            if len(_receipt_paths(log_dir.parent)) >= MAX_ENTRIES:
                raise SlotRefused("phase receipt capacity is full; settle existing work before new acquisition")
            now = tx.clock()
            raw = {"reservation_id": uuid.uuid4().hex, "names": [name], "state": "queued", "purpose": "phase",
                "phase_state": "queued", "owner": {"kind": "task", "task_id": record.task_id}, "generation": 1,
                "enqueued_at": now, "deadline_at": (datetime.fromisoformat(now) + timedelta(seconds=wait_seconds)).isoformat(),
                "execution_generation": execution, "scope_chain": chain,
                "agent_identity": identity.task_identity(record.pid, record.start_time, record.markers),
                "owner_identity": record.owner, "task_authority": {"freedom": record.freedom, "network": record.network, "network_access": (record.enforcement or {}).get("network_access")}, "cleanup_policy": copy.deepcopy(hook),
                "policy_epoch": tx.document["slot_configuration"]["epoch"]}
            # Receipt publication is the conservative admission intent. Interrupted
            # insertion is reconstructed as a blocker, never a grant.
            write_receipt(log_dir.parent, raw)
            tx.entries.append(raw)
            tx.dirty = True
            tx.phase_event(raw, "queued")
            # Zero budget still permits an immediate grant before expiry.
            deadline = raw.pop("deadline_at") if wait_seconds == 0 else None
            tx.grant_pass()
            if deadline is not None:
                raw["deadline_at"] = deadline
                if raw["state"] == "queued":
                    raw["phase_state"] = "expired"
                    tx.phase_event(raw, "expired")
                    tx.entries.remove(raw)
                else:
                    write_receipt(log_dir.parent, raw)
            return _view(tx, raw)
        return ledger._transact(action)


def _view(tx, raw):
    name = raw["names"][0]
    queue = [entry for entry in tx.entries if name in entry["names"] and entry["state"] == "queued"]
    held = [entry for entry in tx.entries if name in entry["names"] and entry["state"] == "held" and not entry.get("borrows_from")]
    capacity = tx.capacity_of(name)
    position = next((index + 1 for index, entry in enumerate(queue) if entry["reservation_id"] == raw["reservation_id"]), None)
    selected = []
    if raw.get("phase_state", raw["state"]) == "queued" and position is not None:
        blockers = {entry["reservation_id"] for entry in queue[:position - 1]}
        if len(held) >= capacity:
            blockers.update(entry["reservation_id"] for entry in held)
        selected = [entry for entry in tx.entries if entry["reservation_id"] in blockers][:100]
    details = [{"reservation_id": entry["reservation_id"],
        "purpose": entry.get("purpose", "whole_task"), "owner": dict(entry["owner"]),
        "state": entry.get("phase_state", entry["state"]),
        "since": entry.get("granted_at") if entry["state"] == "held" else entry["enqueued_at"],
        "enqueued_at": entry["enqueued_at"], "granted_at": entry.get("granted_at"),
        **({"release_requested": True} if entry.get("release_requested") else {})} for entry in selected]
    return {"lease_id": raw["reservation_id"], "task_id": raw["owner"].get("task_id"), "name": name,
        "state": raw.get("phase_state", raw["state"]), "capacity": capacity,
        "enqueued_at": raw["enqueued_at"], "granted_at": raw.get("granted_at"),
        "deadline_at": raw.get("deadline_at"), "cleanup_started_at": raw.get("cleanup_started_at"),
        "cleanup_finished_at": raw.get("cleanup_finished_at"), "diagnostic": raw.get("diagnostic"),
        "overcommitted": len(held) > capacity, "position": position,
        "blocked_by": [entry["reservation_id"] for entry in selected], "blockers": details}


@task_operation
def release(log_dir, lease_id):
    _enabled(log_dir.parent)
    validate_reservation_id(lease_id)
    previous = _authority(log_dir)
    from .task_supervision import read_launch
    with lifecycle_lock(log_dir, previous.task_id):
        record = store.read(log_dir, previous.task_id, include_prompt=False)
        if record is None:
            raise SlotRefused("task metadata is missing; this execution cannot release a lease")
        execution = _generation(record, read_launch(log_dir, previous.task_id))
        def action(tx):
            raw = tx.find(lease_id)
            if raw is None:
                matches = [e for e in _receipts(log_dir.parent) if e["reservation_id"] == lease_id]
                if not matches:
                    return {"lease_id": lease_id, "state": "absent", "ownership_verified": False}
                if matches[0]["owner"].get("task_id") != record.task_id or matches[0]["execution_generation"] != execution:
                    raise SlotRefused("lease does not belong to this execution")
                receipt = matches[0]
                if _unacknowledged_expiry(receipt):
                    receipt["expiry_acknowledged_at"] = tx.clock()
                    tx.phase_event(receipt, "expiry_acknowledged")
                    tx.dirty = True
                return {"lease_id": lease_id, "state": receipt["phase_state"], "acknowledged": receipt.get("expiry_acknowledged_at") is not None}
            if raw.get("purpose") != "phase" or raw["owner"].get("task_id") != record.task_id or raw.get("execution_generation") != execution:
                raise SlotRefused("lease does not belong to this execution")
            outcome = tx.release(lease_id)
            tx.grant_pass()
            return {"lease_id": lease_id, "state": outcome}
        return ResourceLedger(log_dir.parent)._transact(action)


def _runner_inspection(raws):
    observations = [item for raw in raws for item in _observations(raw)]
    runners = {json.dumps(item["cleanup_runner"], sort_keys=True): item["cleanup_runner"] for item in observations if item["cleanup_runner"] is not None}
    for runner in runners.values():
        if identity.identity_check(runner) != "dead":
            raise SlotRefused("recorded cleanup runner remains live or uncertain; inspect and stop its owned work before resolution")
    groups = {}
    for item in observations:
        key = tuple(item[name] for name in ("execution_generation", "cleanup_claim", "cleanup_supervisor"))
        groups.setdefault(key, []).append(item)
    return any(not any(item["cleanup_runner"] is not None or item["cleanup_runner_tracking"] == "not_spawned" for item in group) for group in groups.values())


@task_operation
def resolve(log_dir, lease_id, confirm_untracked_cleanup_stopped=False):
    validate_reservation_id(lease_id)
    ledger, rows, receipts = _slot_snapshot(log_dir)
    observed = next((raw for raw in rows if raw["reservation_id"] == lease_id), None)
    receipt = next((raw for raw in receipts if raw["reservation_id"] == lease_id), None)
    sources = [raw for raw in (observed, receipt) if raw is not None]
    untracked = _runner_inspection(sources)
    if untracked and not confirm_untracked_cleanup_stopped:
        raise SlotRefused("cleanup may have an untracked runner; inspect/stop all owned cleanup descendants and explicitly confirm untracked cleanup stopped")
    inspected = json.dumps(sources, sort_keys=True)
    def action(tx):
        raw = tx.find(lease_id)
        if raw is None or raw.get("purpose") != "phase":
            raise SlotRefused("unknown phase lease")
        current_receipt = next((item for item in _receipts(log_dir.parent) if item["reservation_id"] == lease_id), None)
        current_sources = [item for item in (raw, current_receipt) if item is not None]
        if observed is None or raw["generation"] != observed["generation"] or json.dumps(current_sources, sort_keys=True) != inspected:
            raise SlotRefused("cleanup changed during inspection; retry resolution")
        if raw.get("phase_state") not in ("cleanup_failed", "cleanup_uncertain"):
            raise SlotRefused("only failed/uncertain cleanup may be resolved after inspection")
        if untracked:
            raw["untracked_cleanup_acknowledged_at"] = tx.clock()
            tx.phase_event(raw, "untracked_cleanup_acknowledged")
        raw["phase_state"] = "resolved"
        tx.phase_event(raw, "human_resolved")
        tx.entries.remove(raw)
        tx.dirty = True
        tx.grant_pass()
        return {"lease_id": lease_id, "state": "resolved"}
    return ledger._transact(action)


@task_operation
def status(log_dir, name=None):
    _enabled(log_dir.parent)
    if name is not None:
        validate_names([name])
    ledger, rows, receipts = _slot_snapshot(log_dir)
    visible = rows + [raw for raw in receipts if _unacknowledged_expiry(raw) and not any(item["reservation_id"] == raw["reservation_id"] for item in rows)]
    from .resources import _Transaction
    config = ledger._document["slot_configuration"]
    tx = _Transaction(rows, lambda n: config["names"].get(n, {}).get("capacity", 1), _now)
    leases = [_view(tx, raw) for raw in visible if raw.get("purpose") == "phase" and (name is None or name in raw["names"])]
    return {"leases": leases[:256], "truncated": len(leases) > 256, "configuration": config,
            "service": service_status(log_dir.parent), "events": [e for e in ledger._document["slot_events"] if name is None or e.get("name") == name]}


@task_operation
def _task_view(log_dir, task_id):
    if (_read_marker(log_dir.parent) or {}).get("layout_version") != 2:
        return []
    ledger, rows, receipts = _slot_snapshot(log_dir)
    visible = rows + [raw for raw in receipts if _unacknowledged_expiry(raw) and not any(item["reservation_id"] == raw["reservation_id"] for item in rows)]
    from .resources import _Transaction
    config = ledger._document["slot_configuration"]
    tx = _Transaction(rows, lambda n: config["names"].get(n, {}).get("capacity", 1), _now)
    return [_view(tx, raw) for raw in visible if raw.get("purpose") == "phase" and raw["owner"].get("task_id") == task_id][:1]


@task_operation
def task_view(log_dir, task_id):
    from .resources import ResourceError
    try:
        return _task_view(log_dir, task_id)
    except (ResourceError, OSError, ValueError) as exc:
        return [{"task_id": task_id, "state": "unavailable", "diagnostic": (type(exc).__name__ + ": " + str(exc))[:500]}]


@task_operation
def retained(log_dir, task_id):
    if (_read_marker(log_dir.parent) or {}).get("layout_version") != 2:
        return False
    try:
        return any(raw["owner"].get("task_id") == task_id and raw.get("phase_state") not in FINAL for raw in _receipts(log_dir.parent))
    except SlotSourceChanged:
        return True


def _record_execution(log_dir, task_id):
    from .task_supervision import read_launch
    record = store.read(log_dir, task_id, include_prompt=False)
    if record is None:
        return None, None
    try:
        return record, _generation(record, read_launch(log_dir, task_id))
    except SlotRefused:
        return record, None


def _observed_settled(record):
    return bool(record is not None and record.status in store.TERMINAL_RECORD_STATUSES and record.exit_code is not None and record.output_drained)


def _matches_record_identity(raw, record):
    return bool(record is not None and raw.get("owner_identity") == record.owner
        and raw.get("agent_identity") == identity.task_identity(record.pid, record.start_time, record.markers))


def _confirmed_dead(raw):
    # Process probes may block. Always call before taking a lifecycle lock.
    return (identity.check_detail(raw.get("owner_identity")) == ("dead", "pid_absent")
        and identity.check_detail(raw.get("agent_identity")) == ("dead", "pid_absent"))


@task_operation
def reconcile(log_dir):
    """Publish settlement/expiry intents only; never execute cleanup in a caller."""
    if (_read_marker(log_dir.parent) or {}).get("layout_version") != 2:
        return
    ledger = ResourceLedger(log_dir.parent)
    ledger._transact(lambda tx: None)
    ledger, rows, receipts = _slot_snapshot(log_dir)
    from .task_resources import cancel_requested
    for raw in rows:
        if raw.get("purpose") != "phase":
            continue
        tid = raw["owner"]["task_id"]
        before, execution = _record_execution(log_dir, tid)
        reason = ("task metadata is missing; phase execution cannot be qualified" if before is None else
            "task execution generation changed or is incomplete" if execution != raw["execution_generation"] else
            "task owner/agent identity does not match phase execution evidence" if not _matches_record_identity(raw, before) else None)
        observed = _observed_settled(before)
        cleanup = raw.get("phase_state") in CLEANUP
        dead = False if reason or cleanup or observed else _confirmed_dead(raw)
        with lifecycle_lock(log_dir, tid):
            fresh, current_execution = _record_execution(log_dir, tid)
            if (fresh is None) != (before is None) or current_execution != execution:
                continue
            def settle(tx):
                current = tx.find(raw["reservation_id"])
                if current is None or current["generation"] != raw["generation"]:
                    return
                if reason:
                    if current.get("phase_state") != "cleanup_uncertain" or current.get("diagnostic") != reason:
                        _uncertain(tx, log_dir.parent, current, reason)
                    return
                if cleanup:
                    return
                queued_cancel = current["state"] == "queued" and cancel_requested(log_dir, tid)
                if observed and _observed_settled(fresh) or queued_cancel or dead:
                    tx.release(raw["reservation_id"])
                    tx.grant_pass()
            ledger._transact(settle)
    # An unacknowledged expiry of a live execution remains an idempotent result.
    # Once that exact execution conclusively settles it can no longer retry it.
    for receipt in receipts:
        if not _unacknowledged_expiry(receipt) or any(raw["reservation_id"] == receipt["reservation_id"] for raw in rows):
            continue
        tid = receipt["owner"]["task_id"]
        before, execution = _record_execution(log_dir, tid)
        if before is None or execution != receipt["execution_generation"] or not _matches_record_identity(receipt, before):
            continue
        observed = _observed_settled(before)
        dead = False if observed else _confirmed_dead(receipt)
        if not observed and not dead:
            continue
        with lifecycle_lock(log_dir, tid):
            fresh, current_execution = _record_execution(log_dir, tid)
            if current_execution != execution or observed and not _observed_settled(fresh):
                continue
            def prune(tx):
                if receipt["reservation_id"] in tx.durable_reservation_ids:
                    return
                current = _read_private(log_dir.parent / "phase-leases" / (receipt["reservation_id"] + ".json"))
                if current.get("entry") != receipt:
                    raise SlotSourceChanged("settled expiry changed before pruning; retry")
                _unlink_receipt(log_dir.parent, receipt)
            ledger._transact(prune)
    ledger._transact(lambda tx: tx.grant_pass())


def _service_paths(root):
    return root / "resources" / "slot-service.json", root / "resources" / ".slot-service.lock"


@task_operation
def service_start(log_dir):
    _enabled(log_dir.parent)
    current = service_status(log_dir.parent)
    if current["state"] == "alive":
        return current
    if current["state"] == "undecidable":
        raise SlotRefused("existing supervisor identity is uncertain")
    env = dict(os.environ)
    env["HARFLOW_STATE_ROOT"] = str(log_dir.parent)
    env.pop("HARFLOW_TASK_ID", None)
    process = subprocess.Popen([sys.executable, "-m", "harflow_mcp.phase_slots", "service", str(log_dir.parent)],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        start_new_session=True, close_fds=True, env=env)
    for _ in range(100):
        current = service_status(log_dir.parent)
        if current["state"] == "alive":
            return current
        if process.poll() is not None:
            break
        time.sleep(.02)
    raise SlotRefused("host slot supervisor did not publish qualified startup evidence")


@task_operation
def service_stop(log_dir):
    ledger = ResourceLedger(log_dir.parent)
    if any(raw.get("purpose") == "phase" for raw in ledger._read_only()) or any(e.get("phase_state") not in FINAL for e in _receipts(log_dir.parent)):
        raise SlotRefused("resolve every phase lease before stopping the supervisor")
    path, _ = _service_paths(log_dir.parent)
    receipt = _read_private(path, missing=True)
    if receipt is None:
        return {"state": "missing"}
    if identity.identity_check(receipt.get("identity")) != "alive":
        raise SlotRefused("supervisor identity is not confirmed live")
    # Stop is a durable request read by the exact singleton generation, never a
    # broad process signal. Recheck no leases under the same ledger lock.
    def action(tx):
        if any(e.get("purpose") == "phase" for e in tx.entries):
            raise SlotRefused("a phase lease appeared; resolve it before service-stop")
        receipt["stop_requested"] = True
        _atomic_write(path, receipt)
    ledger._transact(action)
    return {"state": "stop_requested", "generation": receipt["generation"]}


def _publish_spawn_failure(root, original, runner, stopped, diagnostic):
    evidence = copy.deepcopy(original)
    evidence["cleanup_runner_tracking"] = "stopped_untracked" if stopped else "spawn_pending"
    if runner is not None:
        evidence["cleanup_runner"] = runner
    def action(tx):
        current = tx.find(original["reservation_id"])
        if current is None:
            current = copy.deepcopy(evidence)
            current.update(state="held", phase_state="cleanup_uncertain")
            write_receipt(root, current)
            if len(tx.entries) >= MAX_ENTRIES:
                raise SlotRefused("cleanup evidence retained; ledger is full and requires inspection")
            tx.entries.append(current)
        _merge_progress(current, evidence)
        current["cleanup_runner_tracking"] = evidence["cleanup_runner_tracking"]
        _uncertain(tx, root, current, diagnostic, evidence)
    ResourceLedger(root)._transact(action)


def _cleanup(root, lease_id, generation):
    ledger = ResourceLedger(root)
    claim_id = uuid.uuid4().hex
    def claim(tx):
        raw = tx.find(lease_id)
        if raw is None or raw.get("phase_state") != "cleanup_pending":
            return None
        raw.update(phase_state="cleanup_running", cleanup_claim=claim_id, cleanup_supervisor=generation,
                   cleanup_started_at=tx.clock(), cleanup_runner_tracking="spawn_pending")
        tx.phase_event(raw, "cleanup_claimed")
        tx.touch(raw)
        return copy.deepcopy(raw)
    raw = ledger._transact(claim)
    if raw is None:
        return
    result, diagnostic = "cleanup_uncertain", "cleanup result unavailable"
    expected_generation = raw["generation"]
    process = None
    runner = None
    published = False
    try:
        policy = raw["cleanup_policy"]
        # Portable execution checks the configured executable on both sides of
        # spawn. Same-user replacement races are outside this cooperative boundary.
        if _executable(policy["argv"][0]) != policy["executable"]:
            raise SlotRefused("cleanup executable identity changed; inspect before human resolution")
        process = subprocess.Popen(policy["argv"],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, close_fds=True,
            env={k: v for k, v in os.environ.items() if not k.startswith("HARFLOW_")})
        try:
            executable_changed = _executable(policy["argv"][0]) != policy["executable"]
        except (OSError, ValueError):
            executable_changed = True
        runner = identity.capture(process.pid, [])
        if runner is None:
            raise SlotRefused("cleanup runner generation could not be recorded")
        def record_runner(tx):
            current = tx.find(lease_id)
            if (current is None or current.get("phase_state") != "cleanup_running"
                    or current.get("cleanup_supervisor") != generation
                    or current.get("cleanup_claim") != claim_id
                    or current["generation"] != expected_generation):
                raise SlotRefused("cleanup claim changed or became uncertain")
            current.update(cleanup_runner=runner, cleanup_runner_tracking="recorded")
            tx.phase_event(current, "cleanup_started")
            tx.touch(current)
            return current["generation"]
        expected_generation = ledger._transact(record_runner)
        published = True
        if executable_changed:
            raise SlotRefused("cleanup executable changed across spawn; runner may remain live; inspect before human resolution")
        try:
            code = process.wait(timeout=policy["timeout_seconds"])
            result = "released" if code == 0 else "cleanup_failed"
            diagnostic = None if code == 0 else f"cleanup exited {code}; inspect then resolve"
        except subprocess.TimeoutExpired:
            if identity.identity_check(runner) == "alive":
                process.kill()
                process.wait(timeout=5)
            result, diagnostic = "cleanup_uncertain", "cleanup timed out; descendants may survive; inspect then resolve"
    except Exception as exc:
        diagnostic = (type(exc).__name__ + ": " + str(exc))[:800]
        if process is not None and not published:
            stopped = False
            try:
                # This unreaped Popen child belongs to this worker. Its PID cannot
                # be reused before this worker reaps it; no unrelated PID/group is
                # addressed, including when a generation probe was unavailable.
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
                stopped = True
            except (OSError, subprocess.SubprocessError):
                pass
            diagnostic += "; cleanup runner publication failed; inspect owned descendants and confirm untracked cleanup stopped"
            _publish_spawn_failure(root, raw, runner, stopped, diagnostic[:1000])
            return
    def finish(tx):
        current = tx.find(lease_id)
        if (current is None or current.get("phase_state") != "cleanup_running"
                or current.get("cleanup_supervisor") != generation
                or current.get("cleanup_claim") != claim_id
                or current["generation"] != expected_generation):
            return
        if process is None:
            current["cleanup_runner_tracking"] = "not_spawned"
        current.update(phase_state=result, diagnostic=diagnostic, cleanup_finished_at=tx.clock())
        tx.phase_event(current, "cleanup_succeeded" if result == "released" else result)
        tx.touch(current)
        if result == "released":
            tx.entries.remove(current)
            tx.grant_pass()
    ledger._transact(finish)


def _service(root):
    layout = resolve_layout(root=root)
    with state_session(layout):
        _enabled(layout.root)
        directory = layout.root / "resources"
        _directory(directory)
        path, lock_path = _service_paths(layout.root)
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return
            own = identity.capture(os.getpid(), ["harflow_mcp.phase_slots", "service"])
            if own is None:
                return
            generation = uuid.uuid4().hex
            _atomic_write(path, {"product": PRODUCT, "format": "harflow.slot-service", "v": 1,
                "product_version": PRODUCT_VERSION, "identity": own, "generation": generation})
            ledger = ResourceLedger(layout.root)
            workers = {}
            def clean(lease_id):
                try:
                    with state_session(layout):
                        _cleanup(layout.root, lease_id, generation)
                except (ResourceError, OSError, store.TaskDocumentError) as exc:
                    log.warning("Slot cleanup worker deferred: %s", str(exc)[:500])
            last_error = None
            while True:
                try:
                    reconcile(layout.tasks_directory)
                    current = _read_private(path)
                    if current.get("generation") != generation:
                        return
                    if current.get("stop_requested"):
                        path.unlink()
                        _fsync_directory(path.parent)
                        return
                    for raw in ledger._read_only():
                        if raw.get("purpose") == "phase" and raw.get("phase_state") == "cleanup_running":
                            worker = workers.get(raw["reservation_id"])
                            if raw.get("cleanup_supervisor") != generation or worker is None or not worker.is_alive():
                                def lost(tx):
                                    current = tx.find(raw["reservation_id"])
                                    if current and current["generation"] == raw["generation"] and current.get("phase_state") == "cleanup_running":
                                        _uncertain(tx, layout.root, current, "cleanup supervisor/worker lost its result; inspect then resolve")
                                ledger._transact(lost)
                        if raw.get("purpose") == "phase" and raw.get("phase_state") == "cleanup_pending":
                            lease_id = raw["reservation_id"]
                            workers = {key: thread for key, thread in workers.items() if thread.is_alive()}
                            if lease_id not in workers and len(workers) < 32:
                                thread = threading.Thread(target=clean, args=(lease_id,), daemon=True)
                                workers[lease_id] = thread
                                thread.start()
                    last_error = None
                except (ResourceError, OSError, store.TaskDocumentError) as exc:
                    message = type(exc).__name__ + ": " + str(exc)[:500]
                    if message != last_error:
                        log.warning("Slot maintenance deferred; capacity remains protected: %s", message)
                        last_error = message
                time.sleep(.5)
        finally:
            os.close(fd)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "service":
        _service(Path(sys.argv[2]))
