"""Unique id-prefix resolution for task ids and workflow run ids.

A caller may pass a unique prefix of at least `MIN_PREFIX_LENGTH` characters wherever a task id or
a workflow run id is accepted. The rules, in order:

1. Authority first. A managed caller's candidate namespace is exactly the ids its existing guards
   allow for the operation; a direct caller's is the current state root's catalog.
2. An exact match always wins and is returned unchanged. An id in canonical UUID form (hyphenated
   or 32 hex digits) is never treated as a prefix: if it does not exist it is unknown.
3. A value shorter than `MIN_PREFIX_LENGTH` that is not an exact id is `unknown_*` with
   `reason="prefix_too_short"`.
4. Otherwise the value is matched against the names of a *completed* discovery generation of the
   listing catalog (a primary-key range query, never a scan of the records), plus ids held in
   memory by this process. Ids of pending launches that have no record yet match by exact id only.
   If discovery cannot complete the result is `unknown_*` with `reason="prefix_index_unavailable"`;
   a prefix is never guessed.
5. Zero matches is `unknown_*`; one match is the answer once its record file is confirmed to
   still exist; several are `ambiguous_task_id` / `ambiguous_workflow_run_id` with up to
   `MAX_CANDIDATES` candidates.

Callers resolve at the public boundary, rebind the resolved id and never pass the raw input
downstream.
"""

from __future__ import annotations

import os
import re
from typing import Any, Callable, Iterable, NamedTuple

from .state_access import root_operation, task_operation, task_layout

MIN_PREFIX_LENGTH = 8
MAX_CANDIDATES = 5
RUN_READ_BYTES = 4 * 1024 * 1024  # the catalog's own per-record read bound
TREE_MEMBER_LIMIT = 65  # `WorkflowStore.tree_members` default; a result this long may be truncated

_CANONICAL_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}|[0-9a-fA-F]{32}")
_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}")


CODES = frozenset({"unknown_task", "ambiguous_task_id", "unknown_workflow_run", "ambiguous_workflow_run_id"})


class IdResolutionError(Exception):
    """A value that names no single task or run. Not a `ValueError`: transports map it by `code`."""

    def __init__(self, code: str, message: str, data: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.data = {"code": code, **(data or {})}


def is_missing_exact(error: IdResolutionError) -> bool:
    """Whether an `unknown_*` error is only "no such exact id" rather than a failed prefix lookup.

    Surfaces whose contract already answers a missing exact id their own way (approvals, artifacts,
    notifications) pass such a value through unchanged and report only ambiguity and genuine prefix
    failures (`no_prefix_match`, `prefix_index_unavailable`).
    """
    return error.code in {"unknown_task", "unknown_workflow_run"} and error.data.get("reason") in (None, "prefix_too_short")


class Resolution(NamedTuple):
    id: str
    resolved_from: dict[str, str] | None


class _Kind(NamedTuple):
    key: str
    unknown: str
    ambiguous: str
    noun: str
    label_key: str
    suffix: str


TASK = _Kind("task_id", "unknown_task", "ambiguous_task_id", "task", "title", ".meta.json")
RUN = _Kind("workflow_run_id", "unknown_workflow_run", "ambiguous_workflow_run_id", "workflow run", "name", ".json")


class _Unavailable(Exception):
    pass


def is_canonical_uuid(value: str) -> bool:
    return _CANONICAL_UUID.fullmatch(value) is not None


def _label(kind: _Kind) -> str:
    return "task_id" if kind is TASK else "workflow_run_id"


def _unknown(kind: _Kind, value: str, reason: str | None = None) -> IdResolutionError:
    label = _label(kind)
    head = f"unknown {label}: {value}"
    data: dict[str, Any] = {}
    if reason == "prefix_too_short":
        message = f"{head}. It is not an existing {kind.noun} id, and a prefix must be at least {MIN_PREFIX_LENGTH} characters to be resolved."
        data = {"reason": reason, "min_prefix_length": MIN_PREFIX_LENGTH}
    elif reason == "prefix_index_unavailable":
        message = f"{head}. The index needed to resolve a prefix is not ready; retry, or pass the full id."
        data = {"reason": reason}
    elif reason == "no_prefix_match":
        message = f"{head}. No {kind.noun} id starts with it (unique prefixes of at least {MIN_PREFIX_LENGTH} characters are accepted)."
        data = {"reason": reason}
    else:
        message = head + (". Use list_tasks to see what is known." if kind is TASK else ". Use list_workflow_run_page to see what is known.")
    return IdResolutionError(kind.unknown, message, {"input": value, **data})


def _candidates(kind: _Kind, ids: list[str], headers: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for identifier in ids[:MAX_CANDIDATES]:
        header = headers.get(identifier) or {}
        created = header.get("created_at", header.get("started_at", header.get("_stamp")))
        result.append({kind.key: identifier, kind.label_key: header.get(kind.label_key), "status": header.get("status"), "created_at": created})
    return result


def _ambiguous(kind: _Kind, value: str, ids: list[str], headers: dict[str, dict[str, Any]]) -> IdResolutionError:
    truncated = len(ids) > MAX_CANDIDATES
    count = f"more than {MAX_CANDIDATES}" if truncated else str(len(ids))
    return IdResolutionError(kind.ambiguous, f"ambiguous {_label(kind)} prefix {value!r} matches {count} {kind.noun}s; use a longer prefix or the full id",
                             {"input": value, "candidates": _candidates(kind, ids, headers), "truncated": truncated})


def _validate(kind: _Kind, value: Any) -> str:
    if kind is TASK:
        from . import store
        if not isinstance(value, str):
            raise store.InvalidTaskId(f"not a valid task id: {value!r}")
        return store.validate_task_id(value)
    if not isinstance(value, str) or not _RUN_ID.fullmatch(value):
        raise ValueError("Invalid workflow run identifier")
    return value


def _resolve(kind: _Kind, value: Any, *, exists: Callable[[str], bool], match: Callable[[str], tuple[list[str], dict[str, dict[str, Any]]]],
             closed: bool = False) -> Resolution:
    """Resolve `value`; `closed` marks a namespace fixed by the caller's authority.

    A closed namespace never reports `unknown_*`: a value outside it (missing or unauthorized alike)
    is returned unchanged for the existing guard to refuse, so resolution reveals no more than that
    guard did. Only a prefix that is ambiguous within the caller's own namespace is reported.
    """
    value = _validate(kind, value)
    if exists(value):
        return Resolution(value, None)
    if is_canonical_uuid(value):
        if closed:
            return Resolution(value, None)
        raise _unknown(kind, value)
    if len(value) < MIN_PREFIX_LENGTH:
        if closed:
            return Resolution(value, None)
        raise _unknown(kind, value, "prefix_too_short")
    try:
        ids, headers = match(value)
    except _Unavailable:
        if closed:
            return Resolution(value, None)
        raise _unknown(kind, value, "prefix_index_unavailable") from None
    if not ids:
        if closed:
            return Resolution(value, None)
        raise _unknown(kind, value, "no_prefix_match")
    if len(ids) > 1:
        raise _ambiguous(kind, value, ids, headers)
    return Resolution(ids[0], {"input": value, kind.key: ids[0]})


def _present(path: Any) -> bool:
    try:
        os.lstat(path)
    except OSError:
        return False
    return True


def _catalog_match(kind: _Kind, catalog: Any, directory: Any, prefix: str, extra: Iterable[str]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    import sqlite3
    for _attempt in range(2):
        try:
            found = catalog.prefix_names(prefix, MAX_CANDIDATES + 1)
        except (OSError, sqlite3.Error):
            # The catalog is derivative: its failure means "cannot decide", never a guess. Errors of
            # the authoritative state (ownership, compatibility) are not OSError and propagate.
            raise _Unavailable() from None
        if found is None:
            # Concurrent churn kept discovery from completing twice: no decision is possible.
            raise _Unavailable()
        # A name can outlive its file between discovery and now; a missing record never counts.
        present = [name for name in found["names"] if _present(directory / (name + kind.suffix))]
        # A full window that lost names to the filter may have hidden further live matches behind the
        # cut: query again against a fresh discovery, and decide nothing if it happens twice.
        if len(found["names"]) <= MAX_CANDIDATES or len(present) == len(found["names"]):
            break
    else:
        raise _Unavailable()
    ids = set(present)
    ids.update(identifier for identifier in extra if identifier.startswith(prefix))
    return sorted(ids), found["headers"]


def _universe_match(prefix: str, universe: Iterable[str]) -> tuple[list[str], dict[str, dict[str, Any]]]:
    return sorted({identifier for identifier in universe if identifier.startswith(prefix)}), {}


@task_operation(parameter="log_dir")
def resolve_task(log_dir: Any, value: Any, *, managed: tuple[dict[str, Any], dict[str, Any]] | None, in_memory: Callable[[], Iterable[str]] | None = None) -> Resolution:
    """Resolve a task id or unique prefix for a direct (`managed` None) or managed caller.

    A managed caller's prefix namespace is `workflow_inspection.allowed_task_ids`; an exact id that
    exists is returned unchanged so the existing guard refuses it exactly as before.
    """
    from . import store
    memory = set(in_memory()) if in_memory is not None else set()
    allowed: set[str] | None = None
    if managed is not None:
        from .workflow_inspection import allowed_task_ids
        allowed = allowed_task_ids(managed)

    def exists(identifier: str) -> bool:
        if allowed is not None:
            return identifier in allowed
        return (identifier in memory or _present(log_dir / (identifier + store.RECORD_SUFFIX))
                or _present(log_dir / (identifier + ".launch.json")))

    def match(prefix: str) -> tuple[list[str], dict[str, dict[str, Any]]]:
        if allowed is not None:
            return _universe_match(prefix, allowed)
        from .catalog import Catalog
        catalog = Catalog(log_dir, store.RECORD_SUFFIX, layout=task_layout(log_dir))
        return _catalog_match(TASK, catalog, log_dir, prefix, memory)

    return _resolve(TASK, value, exists=exists, match=match, closed=allowed is not None)


@root_operation
def resolve_run(root: Any, value: Any, *, managed: tuple[dict[str, Any], dict[str, Any]] | None = None, scope: str = "own") -> Resolution:
    """Resolve a workflow run id or unique prefix.

    A direct caller (`managed` None) resolves against the root's run catalog. A managed caller's
    namespace is its own run (`scope="own"`) or, for reads that may name linked descendants
    (`scope="tree"`), the bounded persisted tree of its run, each match verified through the
    persisted parent chain.
    """
    from .workflows import WorkflowStore
    storage = WorkflowStore(root)
    members: set[str] | None = None
    if managed is not None:
        own = managed[1]["workflow_run_id"]
        members = {own}
        if scope == "tree":
            listing = storage.tree_members(own)
            # A truncated tree is a partial namespace: never resolve against it.
            if listing["state"] != "ready" or len(listing["members"]) >= TREE_MEMBER_LIMIT:
                members = None
            else:
                members = {member["id"] for member in listing["members"]} | {own}

    def exists(identifier: str) -> bool:
        if managed is not None:
            return members is not None and identifier in members
        return _present(storage.runs / (identifier + ".json"))

    def match(prefix: str) -> tuple[list[str], dict[str, dict[str, Any]]]:
        if managed is not None:
            if members is None:
                raise _Unavailable()
            ids, headers = _universe_match(prefix, members)
            if scope == "tree" and len(ids) == 1 and ids[0] != managed[1]["workflow_run_id"]:
                from .workflow_inspection import _authorized_linked_run
                try:
                    _authorized_linked_run(managed[1], storage.root, ids[0])
                except ValueError:
                    return [], {}
            return ids, headers
        from .catalog import Catalog
        catalog = Catalog(storage.runs, ".json", layout=storage.state_layout)
        return _catalog_match(RUN, catalog, storage.runs, prefix, ())

    return _resolve(RUN, value, exists=exists, match=match, closed=managed is not None)


def resolve_in_set(kind: _Kind, value: Any, universe: Iterable[str]) -> Resolution:
    """Resolve against a closed, already authorized set (run-local tasks, assigned inputs).

    No catalog or file is consulted: an exact member wins and a unique prefix of a member resolves.
    A value that matches nothing is returned unchanged for the caller's own refusal.
    """
    members = set(universe)
    value = _validate(kind, value)
    if value in members:
        return Resolution(value, None)
    if is_canonical_uuid(value):
        return Resolution(value, None)
    return _resolve(kind, value, exists=lambda _identifier: False, match=lambda prefix: _universe_match(prefix, members), closed=True)


def resolve_assigned_input(managed: tuple[dict[str, Any], dict[str, Any]] | None, run_id: str, execution_id: str,
                           source_run_id: str | None, task_id: str | None) -> dict[str, Resolution | None]:
    """Resolve the run, source run and task references of an assigned-input read jointly.

    The caller's own run resolves first. The source run and task then resolve only against the
    caller activation's `authorized_input_refs` for this execution, so a reference from another
    activation, execution or run is never a candidate. Anything but a verified worker is passed
    through unchanged to be refused by the read itself.
    """
    unchanged = {"workflow_run_id": Resolution(run_id, None),
                 "source_run_id": Resolution(source_run_id, None) if source_run_id is not None else None,
                 "task_id": Resolution(task_id, None) if task_id is not None else None}
    if managed is None or managed[0].get("role") != "node":
        return unchanged
    association, run = managed
    activation = next((a for a in run.get("activations", []) if a.get("id") == association.get("activation_id") and a.get("role") == "node"), None)
    own = resolve_in_set(RUN, run_id, {run["workflow_run_id"]})
    if activation is None:
        return {**unchanged, "workflow_run_id": own}
    refs = [ref for ref in activation.get("authorized_input_refs", []) if isinstance(ref, dict) and ref.get("execution_id") == execution_id]
    source = resolve_in_set(RUN, source_run_id, {ref["workflow_run_id"] for ref in refs if isinstance(ref.get("workflow_run_id"), str)}) if source_run_id is not None else None
    target = source.id if source is not None else own.id
    task = resolve_in_set(TASK, task_id, {ref["task_id"] for ref in refs if ref.get("workflow_run_id") == target and isinstance(ref.get("task_id"), str)}) if task_id is not None else None
    return {"workflow_run_id": own, "source_run_id": source, "task_id": task}


@root_operation
def resolve_run_task(root: Any, run_id: str, value: Any) -> Resolution:
    """Resolve a run-local task id against the activations of an already resolved run."""
    from .workflows import WorkflowStore
    value = _validate(TASK, value)
    if is_canonical_uuid(value):
        return Resolution(value, None)
    return resolve_in_set(TASK, value, run_task_ids(WorkflowStore(root).get_run(run_id, metadata_byte_limit=RUN_READ_BYTES)))


def run_task_ids(run: dict[str, Any]) -> set[str]:
    """Every task id recorded in a run's activations, native execution ids included."""
    return {task["task_id"] for activation in run.get("activations", []) for task in activation.get("tasks", []) if isinstance(task, dict) and isinstance(task.get("task_id"), str)}


def echo(result: Any, **resolved: Any) -> Any:
    """Add `resolved_from` (primary id) and `resolved_from_<argument>` entries to a dict reply.

    ``resolved`` maps an argument name to its `Resolution` (or a list of them, for list
    arguments). The primary argument is the one named ``primary``; entries that were not
    prefixes are omitted, and a reply that is not a dict is returned unchanged.
    """
    if not isinstance(result, dict):
        return result
    additions: dict[str, Any] = {}
    for argument, value in resolved.items():
        if value is None:
            continue
        if isinstance(value, Resolution):
            if value.resolved_from is not None:
                additions["resolved_from" if argument == "primary" else f"resolved_from_{argument}"] = value.resolved_from
        else:
            entries = [item.resolved_from for item in value if item.resolved_from is not None]
            if entries:
                additions[f"resolved_from_{argument}"] = entries
    return {**result, **additions} if additions else result
