"""Caller authorization and the single-owner permission control pump."""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from pathlib import Path
from typing import Any

from . import approval_paths, approvals, identity, store
from .approvals import ApprovalError
from .backends.base import ApprovalRefused
from .state_access import storage_operation, task_operation
from .state_ownership import StateOwnershipError, run_owned_worker


def _write_validator(backend: Any):
    validator = getattr(backend, "validate_caller_write_approvals", None)
    if validator is None:
        from .backends.base import UnsupportedCapability
        def validator(write, enabled, version=None):
            if write:
                raise UnsupportedCapability(f"{backend.name} has no qualified caller write approvals")
    return validator


def with_approval_enforcement(backend: Any, enforcement: Any, version: str | None, enabled: bool, write: bool) -> Any:
    shaper = getattr(backend, "approval_enforcement", None)
    return shaper(enforcement, version, enabled, write) if shaper is not None and enabled else enforcement


def transport(backend: Any) -> str | None:
    """The backend's approval transport name; a backend without the seam has none."""
    return getattr(backend, "approval_transport", lambda: None)()


def limitations(backend: Any, write: bool) -> str:
    """The backend's approval limitation text; a backend without the seam claims none."""
    return getattr(backend, "approval_limitations", lambda _write: "Caller approvals are not available for this backend.")(write)


def notice(backend: Any, write: bool) -> str:
    """The bridge notice for a task with caller approvals, from the backend."""
    return getattr(backend, "approval_notice", lambda _write: "Caller approvals use a version-qualified control transport for one exact action.")(write)


def _context(backend: Any, acc: Any) -> dict[str, Any]:
    return getattr(backend, "approval_context", lambda _acc: {})(acc)


def validate(backend: Any, enabled: bool, freedom: str, max_turns: int | None, native: bool = False, write: bool = False) -> None:
    if type(enabled) is not bool:
        raise ValueError("caller_approvals must be a boolean")
    if write or hasattr(backend, "validate_caller_write_approvals"):
        _write_validator(backend)(write, enabled)
    if enabled:
        if not backend.capabilities.supports_caller_approvals:
            from .backends.base import UnsupportedCapability
            raise UnsupportedCapability(f"{backend.name} has no qualified caller approval transport")
        backend.validate_caller_approvals(enabled, freedom, max_turns, native)


async def configure(backend: Any, invocation: Any, enabled: bool, freedom: str, max_turns: int | None, write: bool = False):
    validate(backend, enabled, freedom, max_turns, invocation.native_subagent, write)
    if not enabled:
        return invocation
    version = await asyncio.to_thread(backend.qualify_caller_approvals)
    _write_validator(backend)(write, enabled, version)
    # File reads and Git lookups for a backend's pre-spawn checks must not block the event loop.
    result = await asyncio.to_thread(backend.configure_caller_approvals, invocation, freedom, max_turns, version, write)
    backend.assert_safe(result, freedom)
    return result


@task_operation
def context(directory: Path, task_id: str, standalone_owner: str) -> dict[str, Any]:
    from .workflow_hooks import owner
    receipt = owner(directory, task_id, strict=True)
    result = {"interaction_owner": standalone_owner, "workflow_run_id": None, "root_workflow_run_id": None, "workflow_node_id": None, "workflow_execution_id": None}
    if receipt is None:
        return result
    from .workflows import WorkflowStore
    storage = WorkflowStore(root=directory.parent)
    from types import SimpleNamespace
    budget = SimpleNamespace(metadata_limit=4 * 1024 * 1024, metadata_bytes=0, metadata_decodes=0)
    run = storage.get_run(receipt["workflow_run_id"], metadata_byte_limit=2 * 1024 * 1024, metadata_budget=budget)
    root_id = (run.get("parent_link") or {}).get("root_workflow_run_id") or run["workflow_run_id"]
    root = storage.get_run(root_id, metadata_byte_limit=2 * 1024 * 1024, metadata_budget=budget) if root_id != run["workflow_run_id"] else run
    activation_id = receipt.get("workflow_execution_id") or receipt.get("activation_id")
    activation = next((a for a in run.get("activations", []) if a.get("id") == activation_id), None)
    attempt = next((t for t in activation.get("tasks", []) if t.get("task_id") == task_id), None) if activation else None
    if attempt is None:
        raise ValueError("Approval workflow attempt does not match its ownership receipt")
    if attempt.get("execution_kind") == "native_subagent" or receipt.get("workflow_role") == "native_control":
        raise ValueError("Native approval ownership is unsupported")
    result["hard_deadline"] = attempt.get("timeout_deadline")
    result.update({k: receipt.get(k) for k in ("workflow_run_id", "workflow_node_id", "workflow_execution_id")})
    result["workflow_execution_id"] = receipt.get("workflow_execution_id") or receipt.get("activation_id")
    result.update(root_workflow_run_id=root_id, interaction_owner=root.get("interaction_owner", "caller"))
    return result


@storage_operation
async def authorize(registry: Any, *, interaction_owner: str | None = None) -> None:
    """Authorize the channel role (`caller` or `monitor`); refusals are typed `unauthorized`, `caller_undecidable` or `unsupported`."""
    if os.environ.get("HARFLOW_TASK_ID"):
        raise ApprovalError("unauthorized", "Managed tasks cannot inspect or decide caller permission requests")
    try:
        mutation_caller = await registry._mutation_caller()
    except Exception as exc:
        if getattr(exc, "rule", None) == "caller_undecidable":
            raise ApprovalError("caller_undecidable", str(exc)) from None
        if isinstance(exc, ValueError):
            raise ApprovalError("unauthorized", str(exc)) from None
        raise
    if mutation_caller is not None:
        raise ApprovalError("unauthorized", "Workers and orchestrators cannot inspect or decide caller permission requests")
    if interaction_owner not in {None, "caller", "monitor"}:
        raise ApprovalError("unsupported", "interaction_owner must be caller or monitor")
    if interaction_owner == "monitor":
        from .takeover import caller_refusal
        refusal = await asyncio.to_thread(caller_refusal, registry.log_dir)
        if refusal is not None:
            raise ApprovalError("caller_undecidable" if refusal[0] == "caller_undecidable" else "unauthorized", "The monitor channel is refused for this caller: " + refusal[1])


def _authorize_owner(role: str, owner: str) -> None:
    """A row's owner answers its own rows; the Monitor may also answer caller-owned rows, never the reverse."""
    if role != owner and not (role == "monitor" and owner == "caller"):
        raise ApprovalError("unauthorized", "Approval belongs to the root interaction owner")


@storage_operation
def _record(registry: Any, task_id: str):
    record = store.read(registry.log_dir, task_id, include_prompt=False, metadata_byte_limit=256 * 1024)
    if record is None:
        raise ApprovalError("not_current", "Task not found")
    return record


@storage_operation
def _reconcile_state(registry: Any, task_id: str) -> tuple[Any, bool]:
    """Reconcile pending rows with the task's liveness; also say whether any invalidation was attempted (rows may have changed)."""
    record = _record(registry, task_id)
    current = registry.get(task_id)
    invalidated = False
    # A different still-live server owns its pipe. A restarted server cannot adopt it.
    from .task_attachment import AttachedTask
    if (current is None or isinstance(current, AttachedTask)) and (not record.owner or identity.identity_check(record.owner) != "alive"):
        approvals.invalidate(registry.log_dir, task_id, "invalidated")
        invalidated = True
    if record.status != "running":
        approvals.invalidate(registry.log_dir, task_id, "cancelled" if record.status == "cancelled" else "invalidated")
        invalidated = True
    return record, invalidated


@storage_operation
def _reconcile(registry: Any, task_id: str) -> Any:
    return _reconcile_state(registry, task_id)[0]


@storage_operation
async def get_request(registry: Any, task_id: str, request_id: str, *, interaction_owner: str = "caller") -> dict[str, Any]:
    await authorize(registry, interaction_owner=interaction_owner)
    await asyncio.to_thread(_reconcile, registry, task_id)
    return await asyncio.to_thread(approvals.get, registry.log_dir, task_id, request_id)


def approval_order(row: dict[str, Any]) -> tuple[int, float, str]:
    """Keep actionable requests ahead of retained audit history before paging."""
    delivery = row.get("delivery")
    active = delivery != "delivery_unknown" and (row.get("disposition") == "pending" or delivery in {"reserved", "sending"})
    rank = 0 if active else 1 if delivery == "delivery_unknown" else 2
    return rank, row["created_at"], row["request_id"]


@storage_operation
async def list_requests(registry: Any, task_id: str | None = None, workflow_run_id: str | None = None, cursor: str | None = None, limit: int = 16) -> dict[str, Any]:
    await authorize(registry)
    if (task_id is None) == (workflow_run_id is None):
        raise ValueError("Specify exactly one task_id or workflow_run_id")
    if type(limit) is not int or not 1 <= limit <= 32:
        raise ValueError("limit must be between 1 and 32")
    if cursor is not None and (not isinstance(cursor, str) or not cursor.isdigit() or len(cursor) > 8):
        raise ValueError("Invalid approval cursor")
    offset = int(cursor or 0)
    def collect():
        ids = [task_id] if task_id else workflow_task_ids(registry.log_dir, workflow_run_id)
        result = []
        remaining = 8 * 1024 * 1024
        for identifier in ids:
            path = approvals._path(registry.log_dir, identifier)
            if path.exists():
                remaining -= path.stat().st_size
                if remaining < 0:
                    raise ValueError("Approval page source budget exceeded; inspect a specific task")
            if not path.exists():
                continue
            _reconcile(registry, identifier)
            result.extend(approvals.metadata(r) for r in approvals.rows(registry.log_dir, identifier))
        result.sort(key=approval_order)
        return {"requests": result[offset:offset + limit], "total": len(result), "next_cursor": str(offset + limit) if offset + limit < len(result) else None}
    return await run_owned_worker(registry.state_layout, collect)


@task_operation
def workflow_task_ids(directory: Path, workflow_run_id: str) -> list[str]:
    from .workflows import WorkflowStore
    from types import SimpleNamespace
    storage = WorkflowStore(root=directory.parent)
    budget = SimpleNamespace(metadata_limit=8 * 1024 * 1024, metadata_bytes=0, metadata_decodes=0)
    def read_run(identifier):
        return storage.get_run(identifier, metadata_byte_limit=2 * 1024 * 1024, metadata_budget=budget)
    run = read_run(workflow_run_id)
    root_id = (run.get("parent_link") or {}).get("root_workflow_run_id") or workflow_run_id
    root = read_run(root_id) if root_id != workflow_run_id else run
    pending = [root]
    seen = set()
    ids = []
    while pending:
        current = pending.pop()
        if current["workflow_run_id"] in seen:
            continue
        seen.add(current["workflow_run_id"])
        if len(seen) > 128:
            raise ValueError("Approval workflow tree exceeds bounded traversal")
        for activation in current.get("activations", []):
            invocation = activation.get("invocation") or {}
            child_id = invocation.get("child_workflow_run_id")
            if child_id:
                child = read_run(child_id)
                link = child.get("parent_link") or {}
                if link.get("workflow_run_id") != current["workflow_run_id"] or link.get("execution_id") != activation.get("id") or link.get("root_workflow_run_id") != root_id:
                    raise ValueError("Approval child workflow ownership link is inconsistent")
                pending.append(child)
            for task in activation.get("tasks", []):
                identifier = task.get("task_id")
                if identifier and store.record_path(directory, identifier).exists():
                    ids.append(identifier)
        if len(ids) > 2048:
            raise ValueError("Approval task traversal exceeds bound")
    return list(dict.fromkeys(ids))


@storage_operation
async def respond_request(registry: Any, task_id: str, request_id: str, action_digest: str, owner_revision: int, decision: str, interaction_owner: str = "caller") -> dict[str, Any]:
    """Reserve one decision. `interaction_owner` is the responder's authorizing channel (`caller` or `monitor`), recorded as `responder`.

    Refusals are `ApprovalError`s, raised in a fixed precedence: authorization (`unauthorized`, `caller_undecidable`,
    `unsupported`, then the row owner against the channel), request identity (`not_current`: task, request, digest, owner
    revision, workflow ownership), then state (`settled`, `expired`, `already_answered`), all before any write or
    reconciliation, so they leave the ledger unchanged. Only then is the task reconciled (which may invalidate rows) and
    ownership checked (`ownership_lost`: no new decision is applied, rows may be invalidated). `reserve` repeats the
    identity and state checks under the ledger lock, the compare-and-swap that decides between concurrent responders."""
    if interaction_owner not in approvals.RESPONDERS:
        # A parameter check, evaluated after worker/caller authorization like every other refusal.
        await authorize(registry)
        raise ApprovalError("unsupported", "interaction_owner must be caller or monitor")
    await authorize(registry, interaction_owner=interaction_owner)
    # Parameter validation before any reconciliation; `reserve` repeats it (bool is not an integer revision here).
    if decision not in {"approve", "reject"} or type(owner_revision) is not int:
        raise ApprovalError("unsupported", "A decision must be approve or reject with an integer owner revision")
    record = await asyncio.to_thread(_record, registry, task_id)
    ctx = await asyncio.to_thread(context, registry.log_dir, task_id, record.interaction_owner)
    _authorize_owner(interaction_owner, ctx["interaction_owner"])
    raw = next((r for r in await asyncio.to_thread(approvals.rows, registry.log_dir, task_id) if r["request_id"] == request_id), None)
    if raw is None:
        raise ApprovalError("not_current", "Approval request not found")
    if raw["interaction_owner"] != ctx["interaction_owner"] or raw["root_workflow_run_id"] != ctx["root_workflow_run_id"]:
        raise ApprovalError("not_current", "Approval workflow ownership has changed")
    if raw["action_digest"] != action_digest or raw["owner_revision"] != owner_revision:
        raise ApprovalError("not_current", "Stale approval action, revision or owner generation")
    refusal = approvals.state_refusal(raw, time.time())
    if refusal is not None:
        raise refusal
    record, invalidated = await asyncio.to_thread(_reconcile_state, registry, task_id)
    # Once reconciliation has invalidated anything, the request's own refusal could be its result: report ownership, never a state reason.
    if invalidated:
        raise ApprovalError("ownership_lost", "Approval process ownership is lost or uncertain; the request was reconciled and no decision was applied")
    # Inspect the live identities outside the ledger lock; pump rechecks before write.
    raw = next((r for r in await asyncio.to_thread(approvals.rows, registry.log_dir, task_id) if r["request_id"] == request_id), raw)
    alive = await asyncio.to_thread(lambda: identity.identity_check(raw["bridge_owner"]) == "alive" and identity.identity_check(raw["process_identity"]) == "alive")
    if not alive or record.status != "running" or record.approval_generation != raw["generation"]:
        raise ApprovalError("ownership_lost", "Approval process ownership is lost or uncertain; no decision was applied")
    reserved = await asyncio.to_thread(approvals.reserve, registry.log_dir, task_id, request_id, action_digest, owner_revision, decision, raw["generation"], interaction_owner)
    current = registry.get(task_id)
    from .task_attachment import AttachedTask
    if current is not None and not isinstance(current, AttachedTask):
        current.pump_wake.set()
    # The decision has landed. Answer with the reservation's own snapshot (the shape `approvals.get` returns, private fields
    # removed) instead of re-reading the ledger, so no read after the write can raise a pre-reservation reason.
    return {**approvals.metadata(reserved), "action": reserved["action"], "complete": True}


PATH_CHANGED = "path_changed_since_admission"


def _protection_reason(registry: Any, task: Any, backend: Any, tool: str, found: list[str]) -> str | None:
    """The first protected-path refusal over every path, against the protected targets as they are now (never cached)."""
    targets = approval_paths.protected_targets(task.repo_path, registry.log_dir, extra=backend.protected_config_paths(task.repo_path))
    return next((r for path in found if (r := approval_paths.protection(targets, tool, path, task.repo_path, backend.approval_write_tools())) is not None), None)


def _path_snapshot(task: Any, found: list[str]) -> list[dict[str, str]]:
    """Each path as the backend named it and as `approval_paths.protection` resolves it."""
    return [{"literal": path, "resolved": str(approval_paths.resolve(path, task.repo_path))} for path in found]


def _bounded_rejection(decoded: dict[str, Any], tool: str, found: list[str]) -> dict[str, Any]:
    """A rejection-only audit row for a request whose snapshot would not fit the request budget: ids, tool, size and the
    SHA-256 of the original action, no action input (the existing oversized-request representation), so it is admitted and delivered."""
    import hashlib
    raw = approvals.canonical(decoded["action"])
    bounded = {k: decoded[k] for k in ("native_request_id", "native_method", "tool_use_id") if k in decoded}
    bounded.update(action={"tool_name": tool, "input": {}}, system_rejection="request_too_large", oversize_bytes=len(raw), oversize_action_digest=hashlib.sha256(raw).hexdigest())
    if len(found) == 1 and len(found[0].encode("utf-8")) < 4096:
        bounded["file_path"] = found[0]
    return bounded


def _path_facts(registry: Any, task: Any, decoded: dict[str, Any], backend: Any) -> dict[str, Any]:
    """Repository containment and protected-path refusal, evaluated at every admission (the protected targets are never cached).

    The backend names the paths an action touches and which tools write; one path keeps the established
    `inside_repository` fact, several add `path_facts` (one entry per path) and refuse if any is protected.
    A request that is not already refused also records `path_snapshot`, the resolved paths the pump compares at delivery."""
    found = backend.approval_paths(decoded)
    if not found or decoded.get("system_rejection") == "request_too_large":
        return decoded
    tool = decoded["action"]["tool_name"]
    facts = [{"file_path": path, "inside_repository": approval_paths.inside_repository(task.repo_path, path)} for path in found]
    decoded["inside_repository"] = all(f["inside_repository"] for f in facts)
    if len(found) > 1:
        decoded["path_facts"] = facts
    if decoded.get("system_rejection") is None:
        try:
            reason = _protection_reason(registry, task, backend, tool, found)
        except Exception:
            reason = "protected_path"
        if reason is not None:
            decoded["system_rejection"] = reason
    if decoded.get("system_rejection") is None:
        try:
            snapshot = _path_snapshot(task, found)
        except Exception:
            decoded["system_rejection"] = "protected_path"
        else:
            if len(approvals.canonical({**decoded, "path_snapshot": snapshot})) > approvals.MAX_REQUEST_BYTES:
                return _bounded_rejection(decoded, tool, found)
            decoded["path_snapshot"] = snapshot
    return decoded


def _delivery_path_reason(registry: Any, task: Any, backend: Any, row: dict[str, Any]) -> str | None:
    """Re-check an approved action's paths immediately before the approve is written; None when nothing changed.

    Runs off the event loop (protected targets shell out to Git). A changed resolution, a changed path set or a missing or
    malformed snapshot is `path_changed_since_admission`; a path that is now protected, or that cannot be resolved, is `protected_path`.
    A row that names no paths (a command) has nothing to re-check."""
    try:
        found = backend.approval_paths(row)
        if not found:
            return None
        snapshot = row.get("path_snapshot")
        if not isinstance(snapshot, list) or any(not isinstance(e, dict) or not isinstance(e.get("literal"), str) or not isinstance(e.get("resolved"), str) for e in snapshot):
            return PATH_CHANGED
        if _path_snapshot(task, found) != [{"literal": e["literal"], "resolved": e["resolved"]} for e in snapshot]:
            return PATH_CHANGED
        return _protection_reason(registry, task, backend, row["action"]["tool_name"], found)
    except Exception:
        return "protected_path"


async def _record_audit(registry: Any, task: Any, native_id: str, note: str) -> None:
    """A bounded system audit row (never exposed as a decision) for a stop whose evidence is not a request."""
    audit = {"native_request_id": native_id, "tool_use_id": "unsupported", "action": {"tool_name": "unsupported", "input": {"reason": note[:200]}}}
    ctx = await asyncio.to_thread(context, registry.log_dir, task.task_id, task.interaction_owner)
    row = await asyncio.to_thread(approvals.admit, registry.log_dir, task, audit, ctx)
    await asyncio.to_thread(approvals.update, registry.log_dir, task.task_id, row["request_id"], row["generation"], disposition="unsupported", delivery="failed")


async def _stop_refused(registry: Any, task: Any, exc: Exception) -> None:
    """Version or qualification refusal: notice, invalidate pending approvals and stop the run now."""
    if str(exc) not in task.bridge_notices:
        task.bridge_notices.append(str(exc))
    if task.caller_approvals:
        audit = getattr(exc, "audit", None)
        if audit:
            try:
                if task.start_time is None:
                    captured = await asyncio.wait_for(asyncio.to_thread(identity.capture, task.proc.pid, task.markers), timeout=2.0)
                    if captured is not None:
                        task.start_time = captured["start_time"]
                await _record_audit(registry, task, "audit-" + uuid.uuid4().hex, audit)
            except StateOwnershipError:
                raise
            except Exception:
                pass
        for tool_id, outcome in getattr(exc, "outcomes", ()):
            try:
                for row in await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id):
                    if row["tool_use_id"] == tool_id and row["generation"] == task.approval_generation:
                        await asyncio.to_thread(approvals.observe_outcome, registry.log_dir, task.task_id, row["request_id"], row["generation"], outcome)
            except StateOwnershipError:
                raise
            except Exception:
                pass
        try:
            await asyncio.to_thread(approvals.invalidate, registry.log_dir, task.task_id, "invalidated", task.approval_generation)
        except StateOwnershipError:
            raise
        except Exception:
            pass
    registry.persist(task)
    registry._stop_native_transport(task)


async def _check_version(registry: Any, task: Any, backend: Any, event: dict[str, Any]) -> bool:
    """The init-version cross-check shared by approvals and command grants: a mismatch, a missing version or a tool or
    control event before init stops the run, because the qualified grammar and prompts were measured on one exact version."""
    qualification = {"version": task.caller_approval_version or getattr(task, "allowed_commands_version", None), "observed": getattr(backend, "approval_stream_version", lambda acc: None)(task.acc),
                     "write": task.caller_write_approvals, "feature": "caller approvals" if task.caller_approvals else "allowed_commands", **_context(backend, task.acc)}
    try:
        getattr(backend, "check_approval_event", lambda e, q: None)(event, qualification)
    except ApprovalRefused as exc:
        await _stop_refused(registry, task, exc)
        return False
    return True


@storage_operation
async def observe(registry: Any, task: Any, backend: Any, event: dict[str, Any]) -> bool:
    """Consume control events separately; never emit raw actions in shared events/tails."""
    kind = backend.approval_frame_kind(event)
    if kind == "request":
        if not task.caller_approvals:
            if getattr(task, "allowed_commands", None):
                await _check_version(registry, task, backend, event)
            return False
        task.approval_pending = True
        task.approval_receiving = True
        task.acc.awaiting_input = False
        task.acc.turn_open = True
        try:
            qualification = {"version": task.caller_approval_version, "observed": getattr(backend, "approval_stream_version", lambda acc: None)(task.acc), "write": task.caller_write_approvals, **_context(backend, task.acc)}
            decoded = backend.decode_approval_request(event, qualification) if len(approvals.canonical(event)) <= approvals.MAX_REQUEST_BYTES else getattr(backend, "decode_oversize_approval", lambda e, q: None)(event, qualification)
            if decoded is None:
                raise ValueError("Approval request exceeds 64 KiB")
            decoded = await asyncio.to_thread(_path_facts, registry, task, decoded, backend)
            if task.start_time is None:
                captured = await asyncio.wait_for(asyncio.to_thread(identity.capture, task.proc.pid, task.markers), timeout=2.0)
                if captured is None:
                    raise ValueError("Approval process identity capture unavailable")
                task.start_time = captured["start_time"]
                registry.persist(task)
            ctx = await asyncio.to_thread(context, registry.log_dir, task.task_id, task.interaction_owner)
            await asyncio.to_thread(approvals.admit, registry.log_dir, task, decoded, ctx)
        except StateOwnershipError:
            raise
        except Exception as exc:
            refused = isinstance(exc, ApprovalRefused)
            # Preserve bounded audit evidence even when the action cannot be exposed.
            # Full/invalid ledgers may prevent this final record; cancellation remains mandatory.
            try:
                native_id = backend.approval_request_key(event)
                if not isinstance(native_id, str) or not native_id or len(native_id) > 256:
                    native_id = "uncorrelated-" + task.approval_generation
                audit = {"native_request_id": native_id, "tool_use_id": "unsupported", "action": {"tool_name": "unsupported", "input": {}}}
                ctx = await asyncio.to_thread(context, registry.log_dir, task.task_id, task.interaction_owner)
                row = await asyncio.to_thread(approvals.admit, registry.log_dir, task, audit, ctx)
                await asyncio.to_thread(approvals.update, registry.log_dir, task.task_id, row["request_id"], row["generation"], disposition="unsupported", delivery="failed")
            except StateOwnershipError:
                raise
            except Exception:
                pass
            task.bridge_notices.append(str(exc) if refused else "Unsupported, invalid or over-budget permission request; stopping execution without approval")
            if refused:
                try:
                    await asyncio.to_thread(approvals.invalidate, registry.log_dir, task.task_id, "invalidated", task.approval_generation)
                except StateOwnershipError:
                    raise
                except Exception:
                    pass
            registry.persist(task)
            registry._stop_native_transport(task)
        task.approval_view = await asyncio.to_thread(approvals.summary, registry.log_dir, task.task_id)
        task.approval_receiving = False
        task.pump_wake.set()
        return True
    if (task.caller_approvals or getattr(task, "allowed_commands", None)) and not await _check_version(registry, task, backend, event):
        return False
    if task.caller_approvals:
        cancelled = backend.approval_request_key(event) if kind in {"cancellation", "resolved"} else None
        if cancelled is not None:
            for row in await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id):
                # A "resolved" frame is the harness acknowledging an answer Harflow is writing or has written:
                # only a request that is still unanswered loses its eligibility. A cancellation closes any pending row.
                if kind == "resolved" and row["delivery"] not in {"pending", "reserved"}:
                    continue
                if row["native_request_id"] == cancelled and row["generation"] == task.approval_generation and row["disposition"] == "pending":
                    await asyncio.to_thread(approvals.observe_cancellation, registry.log_dir, task.task_id, row["request_id"], row["generation"])
        for tool_id, outcome in getattr(backend, "observed_approval_outcomes", lambda e, _acc: backend.approval_outcomes(e))(event, task.acc):
            for row in await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id):
                if row["tool_use_id"] == tool_id and row["generation"] == task.approval_generation:
                    await asyncio.to_thread(approvals.observe_outcome, registry.log_dir, task.task_id, row["request_id"], row["generation"], outcome)
    return False


@storage_operation
async def pump(registry: Any, task: Any) -> bool:
    """Return whether verified requests still require an open stdin pipe."""
    if not task.caller_approvals:
        return False
    from .tasks import _write_stdin
    from .backends import get
    records = await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id)
    for row in records:
        if row["generation"] != task.approval_generation or row["disposition"] != "pending":
            continue
        if task.cancel_requested or task.proc is None or task.proc.returncode is not None or task.input_closed or getattr(getattr(task, "acc", None), "error_result_seen", False):
            await asyncio.to_thread(approvals.invalidate, registry.log_dir, task.task_id, "cancelled" if task.cancel_requested else "invalidated", task.approval_generation)
            return False
        expired = row["deadline"] <= time.time()
        if row["delivery"] == "pending" and expired:
            # System expiry is distinct from a caller's recorded decision.
            row = await asyncio.to_thread(approvals.update, registry.log_dir, task.task_id, row["request_id"], row["generation"], "pending", effective_decision="reject", delivery="reserved")
        if row["delivery"] != "reserved":
            continue
        verified = await asyncio.to_thread(lambda: identity.identity_check(row["process_identity"]) == "alive" and identity.identity_check(row["bridge_owner"]) == "alive")
        ctx = await asyncio.to_thread(context, registry.log_dir, task.task_id, task.interaction_owner)
        if not verified or row["bridge_owner"] != task.owner or row["session_id"] != task.session_id or row["action_digest"] != approvals.digest(row["action"]) or any(row.get(k) != ctx.get(k) for k in ctx):
            await asyncio.to_thread(approvals.invalidate, registry.log_dir, task.task_id, "invalidated", task.approval_generation)
            registry._stop_native_transport(task)
            return False
        # Delivery-time path re-check: an approve is written only for paths that still resolve as admitted and are not protected now.
        # The conversion is its own CAS, before the send reservation, so a crash between the two can never leave an approve to write.
        if (row.get("effective_decision") or row["decision"]) == "approve" and not row.get("system_rejection"):
            reason = await asyncio.to_thread(_delivery_path_reason, registry, task, get(task.backend), row)
            if reason is not None:
                try:
                    row = await asyncio.to_thread(approvals.update, registry.log_dir, task.task_id, row["request_id"], row["generation"], "reserved", system_rejection=reason, effective_decision="reject")
                except ValueError:
                    current = next((r for r in await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id) if r["request_id"] == row["request_id"]), None)
                    if current is not None and current["generation"] == task.approval_generation and (current["execution_outcome"] != "unknown" or current["disposition"] != "pending"):
                        continue
                    raise
        # Durable send reservation precedes the only write. A crash in this window is
        # ambiguous and recovery never retries it. The lock is released before awaiting.
        # Cancellation is checked again after the off-loop CAS and before the synchronous write.
        try:
            row = await asyncio.to_thread(approvals.update, registry.log_dir, task.task_id, row["request_id"], row["generation"], "reserved", delivery="sending")
        except ValueError:
            current = next((r for r in await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id) if r["request_id"] == row["request_id"]), None)
            if current is not None and current["generation"] == task.approval_generation and (current["execution_outcome"] != "unknown" or current["disposition"] != "pending"):
                continue
            raise
        if task.cancel_requested or task.proc.returncode is not None or task.input_closed or getattr(getattr(task, "acc", None), "error_result_seen", False):
            await asyncio.to_thread(approvals.invalidate, registry.log_dir, task.task_id, "cancelled" if task.cancel_requested else "invalidated", task.approval_generation)
            return False
        effective = row.get("effective_decision") or row["decision"]
        wire = get(task.backend).encode_approval_response(row, effective)
        # Identity/workflow/ledger checks may cross the deadline. This check and
        # the write are synchronous, so expiry cannot interleave through an await.
        expiry_rejection = row.get("expiry_rejection", False) or row["deadline"] <= time.time()
        if expiry_rejection:
            effective = "reject"
            wire = get(task.backend).encode_approval_response({**row, "expiry_rejection": True}, effective)
        written = _write_stdin(task, wire)
        try:
            # Bookkeeping for denial classification must never change an outcome.
            getattr(get(task.backend), "note_approval_answer", lambda *_a: None)(task.acc, {**row, "expiry_rejection": expiry_rejection}, effective, written)
        except Exception:
            pass
        await asyncio.to_thread(approvals.finish_delivery, registry.log_dir, task.task_id, row["request_id"], row["generation"], written, effective, expiry_rejection)
        # A successful local write is not a harness acknowledgement. Observed tool
        # outcomes are updated only by the stdout request/tool identity correlation.
    task.approval_pending = task.approval_receiving or any(r["disposition"] == "pending" and r["delivery"] in approvals.ACTIVE for r in await asyncio.to_thread(approvals.rows, registry.log_dir, task.task_id))
    task.approval_view = await asyncio.to_thread(approvals.summary, registry.log_dir, task.task_id)
    return task.approval_pending


@task_operation
def workflow_summary(directory: Path, run_id: str) -> dict[str, Any]:
    """Bounded metadata only, aggregated through reciprocal root-owned child links."""
    try:
        records = []
        remaining = 8 * 1024 * 1024
        uncertain = False
        uncertainty_records = []
        for task_id in workflow_task_ids(directory, run_id):
            path = approvals._path(directory, task_id)
            if path.exists():
                remaining -= path.stat().st_size
                if remaining < 0:
                    raise ValueError("Approval summary source budget exceeded")
            view = approvals.summary(directory, task_id)
            uncertain |= view["interaction_state"] == "approval_state_uncertain"
            records.extend(view["requests"])
            uncertainty_records.extend(view.get("uncertainty_requests", []))
        records.sort(key=lambda row: (row["created_at"], row["request_id"]))
        return {"pending_count": len(records), "awaiting_decision_count": approvals.awaiting_count(records), "delivery_uncertain_count": len(uncertainty_records), "uncertainty_requests": uncertainty_records[:32], "interaction_state": "approval_state_uncertain" if uncertain else "waiting_for_approval" if records else None, "requests": records[:32], "has_more": len(records) > 32}
    except (OSError, ValueError, KeyError, TypeError):
        return {"pending_count": 0, "awaiting_decision_count": 0, "interaction_state": "approval_state_uncertain", "requests": [], "error": "Workflow approval ownership is unavailable"}
