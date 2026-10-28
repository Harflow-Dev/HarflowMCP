"""Qualified caller liveness and human handover, independent of scheduler ownership."""
from __future__ import annotations

import copy
import os
import subprocess
import time
from typing import Any

from . import identity

_transport_caller: dict[str, Any] | None = None


def validate_caller(value: Any, path: str = "interaction_caller") -> None:
    if type(value) is not dict or value.get("kind") not in {"managed_task", "external_parent", "unknown"}:
        raise ValueError(f"Malformed {path}")
    expected = {"kind", "identity", "task_id", "session_id"} if value["kind"] == "managed_task" else {"kind", "identity"}
    if set(value) != expected:
        raise ValueError(f"Malformed {path}")
    if value["kind"] == "managed_task":
        from .workflow_run_documents import identifier
        identifier(value["task_id"])
        if value["session_id"] is not None and (type(value["session_id"]) is not str or not value["session_id"]):
            raise ValueError(f"Malformed {path}.session_id")
    evidence = value["identity"]
    if value["kind"] == "unknown":
        if evidence is not None:
            raise ValueError(f"Malformed {path}")
        return
    if type(evidence) is not dict or set(evidence) != {"pid", "start_time", "markers"} or type(evidence["pid"]) is not int or evidence["pid"] <= 1 or type(evidence["start_time"]) is not str or not evidence["start_time"].strip() or type(evidence["markers"]) is not list or any(type(m) is not str or not m for m in evidence["markers"]):
        raise ValueError(f"Malformed {path}.identity")


def capture_transport_parent() -> dict[str, Any]:
    """Capture before any detach; wrapper processes cannot establish caller death.

    Known transparent launchers are traversed with revalidated parent links.
    Shell and interpreter parents are unknown: their lifetime need not match
    the application that invoked them.
    """
    global _transport_caller
    from . import lineage
    direct_parent = os.getppid()
    result = {"kind": "unknown", "identity": None}
    try:
        table = lineage.process_table()
        if table is None:
            raise ValueError("Parent ancestry unavailable")
        parent = direct_parent
        wrappers = {"uv", "uvx", "env", "nohup", "setsid"}
        unsupported = {"sh", "bash", "zsh", "fish", "dash", "npx", "npm"}
        captures = []
        for _ in range(12):
            if parent <= 1:
                break
            evidence = identity.capture(parent, [])
            if not evidence or not evidence.get("start_time"):
                break
            proc = subprocess.run(["ps", "-o", "comm=", "-p", str(parent)], capture_output=True, text=True, timeout=5, check=False)
            name = os.path.basename(proc.stdout.strip()).lower()
            if proc.returncode or not name:
                break
            captures.append(evidence)
            if name in wrappers:
                parent = table.get(parent, 0)
                continue
            if name in unsupported or name.startswith(("python", "node", "ruby", "perl")):
                break
            fresh = lineage._load_process_table()
            chain = [item["pid"] for item in captures]
            if fresh is not None and os.getppid() == direct_parent and all(fresh.get(a) == b for a, b in zip(chain, chain[1:])) and all(identity.check_detail(item)[0] == "alive" for item in captures):
                result = {"kind": "external_parent", "identity": evidence}
            break
        validate_caller(result)
    except (OSError, subprocess.SubprocessError, ValueError):
        result = {"kind": "unknown", "identity": None}
    _transport_caller = result
    return copy.deepcopy(result)


def admission(caller: Any = None) -> dict[str, Any]:
    if caller is not None:
        record = caller.record
        value = {"kind": "managed_task", "task_id": record.task_id, "session_id": record.session_id, "identity": identity.task_identity(record.pid, record.start_time, record.markers)}
        try:
            validate_caller(value)
            return value
        except ValueError:
            return {"kind": "unknown", "identity": None}
    return copy.deepcopy(_transport_caller or {"kind": "unknown", "identity": None})


def authority_root(storage: Any, run: dict[str, Any]) -> dict[str, Any]:
    root_id = (run.get("parent_link") or {}).get("root_workflow_run_id")
    if not root_id or root_id == run["workflow_run_id"]:
        return run
    from .catalog import Catalog
    budget = Catalog(storage.runs, ".json", layout=storage.state_layout)
    return storage.get_run(root_id, refresh_atomic=True, metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)


def owner_state(run: dict[str, Any], storage: Any = None) -> tuple[str, str]:
    value = run.get("interaction_caller")
    if value is None:
        return "unknown", "Caller identity was not recorded at admission"
    validate_caller(value)
    if value["kind"] == "unknown":
        return "unknown", "Caller process could not be qualified"
    verdict, reason = identity.check_detail(value["identity"])
    if verdict != "dead":
        return ("unknown" if verdict == "undecidable" else verdict), reason
    if reason not in {"pid_absent", "start_time_differs"}:
        return "unknown", reason
    if value["kind"] == "managed_task":
        from . import store
        if storage is None:
            return "unknown", "Managed caller record cannot be verified"
        try:
            from .catalog import Catalog
            budget = Catalog(storage.root / "tasks", store.RECORD_SUFFIX, layout=storage.state_layout)
            def read_caller(task_id):
                return store.read(storage.root / "tasks", task_id, include_prompt=False,
                                  metadata_byte_limit=4 * 1024 * 1024, metadata_budget=budget)
            record = read_caller(value["task_id"])
            if record is None or identity.task_identity(record.pid, record.start_time, record.markers) != value["identity"]:
                return "unknown", "Managed caller record changed or is unavailable"
            session = value["session_id"] or record.session_id
            if session:
                if record.session_id != session:
                    return "unknown", "Managed caller session changed"
                ancestors = set()
                parent_id = record.parent_task_id
                while parent_id:
                    if parent_id == record.task_id or parent_id in ancestors or len(ancestors) >= 100:
                        return "unknown", "Managed caller ancestry is cyclic or exceeds its bounded budget"
                    parent = read_caller(parent_id)
                    if parent is None or parent.session_id not in {None, session}:
                        return "unknown", "Managed caller ancestry is incomplete or changed"
                    ancestors.add(parent_id)
                    parent_id = parent.parent_task_id
                page = store.list_page(storage.root / "tasks", session_id=session, limit=100)
                if page.get("bootstrap_pending") or page.get("has_more") or page.get("history_incomplete") or any(item.get("task_id") not in ancestors | {record.task_id} for item in page.get("items", [])):
                    return "unknown", "Managed caller session continued or its history is incomplete"
                from itertools import islice
                if len(list(islice((storage.root / "tasks").glob("*.launch.json"), 101))) > 100:
                    return "unknown", "Managed caller reservation check exceeds its bounded budget"
                from .task_supervision import session_reservations
                if any(item.get("session_id") == session for item in session_reservations(storage.root / "tasks")):
                    return "unknown", "Managed caller session has a reserved continuation"
        except Exception:
            # Failed qualification never proves that a managed caller disappeared.
            return "unknown", "Managed caller session cannot be verified"
    return verdict, reason


def authorize(storage: Any, run: dict[str, Any], responder: str | None, action: str, *, pending: bool = False) -> None:
    """Called inside an accepted mutation; exceptions leave handover unpublished.

    A None responder is reserved for existing trusted internal storage callers;
    public transports normalize omitted ownership to caller before invoking this.
    The transport separately verifies Monitor's human authority. Children inherit
    root authority and never mutate the root while holding a descendant run lock.
    """
    from .workflows import WorkflowError
    root = authority_root(storage, run)
    owner = root.get("interaction_owner", "caller")
    if responder is None:
        # Retain legacy trusted calls, but a handover cannot be bypassed by
        # a returning caller omitting its owner on a later checkpoint.
        if not root.get("interaction_handover"):
            return
        responder = "caller"
    if responder == owner:
        return
    if responder != "monitor" or owner != "caller" or action not in {"resume", "recover", "revise_goal", "message", "cancel_message"}:
        raise WorkflowError("Workflow interaction belongs to its original caller")
    state, reason = owner_state(root, storage)
    if state != "dead":
        raise WorkflowError("Workflow interaction belongs to its original caller; caller is " + state)
    if not pending and root.get("status") not in {"needs_input", "needs_attention", "paused", "failed", "starting", "running", "cancelling"}:
        raise WorkflowError("No pending workflow interaction can be handed over")
    if root is not run:
        raise WorkflowError("Claim the pending interaction through the root workflow")
    run["interaction_owner"] = "monitor"
    run["interaction_handover"] = {"timestamp": time.time(), "reason": reason, "previous_owner": "caller", "responder": "monitor", "action": action}


def projection(storage: Any, run: dict[str, Any]) -> dict[str, Any]:
    root = authority_root(storage, run)
    state, reason = owner_state(root, storage)
    owner = root.get("interaction_owner", "caller")
    pending = run.get("status") in {"needs_input", "needs_attention", "paused", "failed"} or bool(run.get("pending_inputs") or run.get("pending_attention"))
    if run.get("status") in {"starting", "running", "cancelling"} and root.get("supervisor_identity"):
        pending = pending or identity.check_detail(root["supervisor_identity"])[0] == "dead"
    available = pending and (owner == "monitor" or state == "dead")
    return {"interaction_state": {"owner": owner, "caller_state": state, "monitor_available": available, "reason": "" if available else reason if state != "alive" else "Original caller is alive", "handover": copy.deepcopy(root.get("interaction_handover"))}}


def validate_handover(value: Any) -> None:
    import math
    if type(value) is not dict or set(value) != {"timestamp", "reason", "previous_owner", "responder", "action"} or type(value["timestamp"]) not in (int, float) or not math.isfinite(value["timestamp"]) or value["timestamp"] <= 0 or value["previous_owner"] != "caller" or value["responder"] != "monitor" or value["action"] not in {"resume", "recover", "revise_goal", "message", "cancel_message"} or type(value["reason"]) is not str or value["reason"] not in {"pid_absent", "start_time_differs"}:
        raise ValueError("Malformed interaction_handover")
