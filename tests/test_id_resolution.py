"""Unique id-prefix resolution for task ids and workflow run ids (#80)."""

from __future__ import annotations

import inspect
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from mcp import MCPError

from harflow_mcp import catalog, ctl, id_resolution, server, state_ownership, store, workflow_inspection
from harflow_mcp.state_layout import resolve_layout
from harflow_mcp.id_resolution import IdResolutionError, MAX_CANDIDATES, MIN_PREFIX_LENGTH, Resolution
from harflow_mcp.tasks import default_log_dir as ctl_log_dir

UUID_A = "11111111-aaaa-4aaa-8aaa-111111111111"
UUID_B = "11111111-bbbb-4bbb-8bbb-222222222222"


def make_record(task_id: str, **overrides) -> store.TaskRecord:
    now = datetime.now(timezone.utc).isoformat()
    base = {"task_id": task_id, "backend": "claude", "session_id": "s-" + task_id, "markers": ["s-" + task_id],
            "repo_path": "/tmp/repo", "started_at": now, "status": "completed", "exit_code": 0, "finished_at": now,
            "title": "title of " + task_id,
            "owner": {"pid": 4242, "start_time": "Wed Jan  1 00:00:00 2000", "markers": []}}
    return store.TaskRecord(**(base | overrides))


def write_tasks(log_dir: Path, *task_ids: str) -> None:
    for task_id in task_ids:
        store.write(log_dir, make_record(task_id))


def write_runs(root: Path, *run_ids: str) -> Path:
    runs = root / "workflow-runs"
    runs.mkdir(parents=True, exist_ok=True)
    for run_id in run_ids:
        (runs / f"{run_id}.json").write_text("{}")
    return runs


def node_view(*task_ids: str, run_id: str = "run-own-000000001") -> tuple[dict, dict]:
    return (
        {"role": "node", "activation_id": "exec"},
        {"workflow_run_id": run_id, "activations": [
            {"id": "exec", "role": "node", "status": "running", "tasks": [{"task_id": task_id, "status": "running"} for task_id in task_ids]}]},
    )


@pytest.fixture(autouse=True)
def owned_server_state(tmp_path: Path):
    with state_ownership.state_session(resolve_layout(root=tmp_path / "state")):
        pass


@pytest.fixture
def log_dir() -> Path:
    return server._reg().log_dir


@pytest.fixture
def root(log_dir: Path) -> Path:
    return log_dir.parent


def resolve(log_dir: Path, value: str, **kwargs):
    return id_resolution.resolve_task(log_dir, value, managed=kwargs.pop("managed", None), **kwargs)


# --- tasks: matching rules -------------------------------------------------------------------


def test_unique_prefix_resolves_and_reports_the_input(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A, UUID_B)
    assert resolve(log_dir, UUID_A[:12]) == Resolution(UUID_A, {"input": UUID_A[:12], "task_id": UUID_A})
    assert resolve(log_dir, "11111111-bbbb") .id == UUID_B


def test_exact_id_is_returned_unchanged_without_an_echo(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    assert resolve(log_dir, UUID_A) == Resolution(UUID_A, None)


def test_exact_custom_id_wins_over_a_longer_id_it_prefixes(log_dir: Path) -> None:
    write_tasks(log_dir, "custom-id", "custom-id-extended")
    assert resolve(log_dir, "custom-id") == Resolution("custom-id", None)
    assert resolve(log_dir, "custom-id-e").id == "custom-id-extended"


def test_short_exact_custom_id_resolves_even_below_the_minimum(log_dir: Path) -> None:
    write_tasks(log_dir, "t1", "t1-and-more-chars")
    assert resolve(log_dir, "t1") == Resolution("t1", None)


def test_a_value_below_the_minimum_that_is_not_an_id_is_too_short(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    short = UUID_A[: MIN_PREFIX_LENGTH - 1]
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, short)
    assert caught.value.code == "unknown_task"
    assert caught.value.data["reason"] == "prefix_too_short"
    assert caught.value.data["min_prefix_length"] == MIN_PREFIX_LENGTH
    assert str(MIN_PREFIX_LENGTH) in str(caught.value)


def test_no_match_is_unknown_with_a_reason(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "ffffffff-ffff")
    assert (caught.value.code, caught.value.data["reason"]) == ("unknown_task", "no_prefix_match")


def test_ambiguous_prefix_lists_candidates_with_metadata(log_dir: Path) -> None:
    write_tasks(log_dir, "shared-prefix-1", "shared-prefix-2")
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "shared-prefix")
    error = caught.value
    assert error.code == "ambiguous_task_id"
    assert error.data["truncated"] is False
    assert [item["task_id"] for item in error.data["candidates"]] == ["shared-prefix-1", "shared-prefix-2"]
    assert error.data["candidates"][0]["title"] == "title of shared-prefix-1"
    assert set(error.data["candidates"][0]) == {"task_id", "title", "status", "created_at"}


def test_ambiguity_is_capped_and_marks_truncation(log_dir: Path) -> None:
    ids = [f"many-prefix-{number}" for number in range(MAX_CANDIDATES + 2)]
    write_tasks(log_dir, *ids)
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "many-prefix")
    assert len(caught.value.data["candidates"]) == MAX_CANDIDATES
    assert caught.value.data["truncated"] is True


def test_exactly_the_cap_is_not_truncated(log_dir: Path) -> None:
    write_tasks(log_dir, *[f"cap-prefix-{number}" for number in range(MAX_CANDIDATES)])
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "cap-prefix")
    assert len(caught.value.data["candidates"]) == MAX_CANDIDATES
    assert caught.value.data["truncated"] is False


def test_canonical_uuid_is_never_a_prefix(log_dir: Path) -> None:
    # A 32-hex id exists; its full-length canonical sibling that does not exist must not match it.
    write_tasks(log_dir, "a" * 32 + "-longer-custom-id")
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "a" * 32)
    assert caught.value.code == "unknown_task"
    assert "reason" not in caught.value.data
    with pytest.raises(IdResolutionError):
        resolve(log_dir, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")


@pytest.mark.parametrize("value", ["../escape", "a/b", "", ".hidden", "has space", "x" * 65])
def test_invalid_characters_keep_the_existing_error(log_dir: Path, value: str) -> None:
    with pytest.raises(store.InvalidTaskId):
        resolve(log_dir, value)


def test_other_roots_are_never_matched(tmp_path: Path, log_dir: Path) -> None:
    other = tmp_path / "other-root" / "tasks"
    with state_ownership.state_session(resolve_layout(root=tmp_path / "other-root")):
        pass
    write_tasks(other, UUID_B)
    write_tasks(log_dir, UUID_A)
    with pytest.raises(IdResolutionError):
        resolve(log_dir, UUID_B[:12])
    assert resolve(other, UUID_B[:12]).id == UUID_B


def test_in_memory_ids_match_and_count(log_dir: Path) -> None:
    assert resolve(log_dir, "memory-only-task", in_memory=lambda: ["memory-only-task"]) == Resolution("memory-only-task", None)
    assert resolve(log_dir, "memory-only", in_memory=lambda: ["memory-only-task"]).id == "memory-only-task"
    write_tasks(log_dir, "memory-only-disk")
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "memory-only", in_memory=lambda: ["memory-only-task"])
    assert caught.value.code == "ambiguous_task_id"


def test_pending_launches_match_by_exact_id_only(log_dir: Path) -> None:
    write_tasks(log_dir, "unrelated-task")
    (log_dir / "pending-launch-1.launch.json").write_text("{}")
    assert resolve(log_dir, "pending-launch-1") == Resolution("pending-launch-1", None)
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "pending-launch")
    assert caught.value.data["reason"] == "no_prefix_match"


def test_a_name_whose_record_vanished_after_discovery_is_not_a_match(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    assert resolve(log_dir, UUID_A[:12]).id == UUID_A  # discovery has now seen the name
    store.record_path(log_dir, UUID_A).unlink()
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, UUID_A[:12])
    assert caught.value.code == "unknown_task"


def test_names_of_an_older_scan_generation_do_not_match(log_dir: Path) -> None:
    write_tasks(log_dir, "present-task-1")
    resolve(log_dir, "present-task")
    index = catalog.Catalog(log_dir, store.RECORD_SUFFIX, layout=id_resolution.task_layout(log_dir))
    with index.connect() as db:
        db.execute("INSERT OR REPLACE INTO seen VALUES ('stale-ghost-task', 0)")
        generation = int(db.execute("SELECT value FROM state WHERE key='scan_generation'").fetchone()[0])
    assert generation > 0
    (log_dir / "stale-ghost-task.meta.json").write_text("{}")  # file present, but only a stale seen row names it
    # The directory changed, so discovery rescans: the ghost now belongs to the current generation.
    assert resolve(log_dir, "stale-ghost").id == "stale-ghost-task"
    (log_dir / "stale-ghost-task.meta.json").unlink()
    with index.connect() as db:
        db.execute("INSERT OR REPLACE INTO seen VALUES ('stale-ghost-task', 0)")
    with pytest.raises(IdResolutionError):
        resolve(log_dir, "stale-ghost")


def test_a_prefix_is_never_guessed_when_discovery_cannot_complete(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tasks(log_dir, UUID_A)
    calls = []

    def churning(self, db):
        calls.append(1)
        db.execute("DELETE FROM state WHERE key='complete'")

    monkeypatch.setattr(catalog.Catalog, "_discover", churning)
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, UUID_A[:12])
    assert caught.value.code == "unknown_task"
    assert caught.value.data["reason"] == "prefix_index_unavailable"
    assert len(calls) == 2  # retried once


def test_discovery_that_completes_on_the_retry_resolves(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tasks(log_dir, UUID_A)
    original = catalog.Catalog._discover
    calls = []

    def flaky(self, db):
        calls.append(1)
        if len(calls) == 1:
            db.execute("DELETE FROM state WHERE key='complete'")
            return
        original(self, db)

    monkeypatch.setattr(catalog.Catalog, "_discover", flaky)
    assert resolve(log_dir, UUID_A[:12]).id == UUID_A


def test_a_task_created_during_lookup_never_turns_into_a_different_task(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tasks(log_dir, "concurrent-aaa")
    original = catalog.Catalog.prefix_names

    def create_after_lookup(self, prefix, limit):
        found = original(self, prefix, limit)
        write_tasks(log_dir, "concurrent-bbb")
        return found

    monkeypatch.setattr(catalog.Catalog, "prefix_names", create_after_lookup)
    assert resolve(log_dir, "concurrent").id == "concurrent-aaa"  # the snapshot the lookup decided on
    monkeypatch.setattr(catalog.Catalog, "prefix_names", original)
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "concurrent")
    assert caught.value.code == "ambiguous_task_id"


def test_lookup_is_a_primary_key_range_not_a_pattern_scan(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tasks(log_dir, "plan-check-1")
    statements = []
    original_connect = catalog.Catalog.connect

    def traced(self):
        db = original_connect(self)
        db.set_trace_callback(statements.append)
        return db

    monkeypatch.setattr(catalog.Catalog, "connect", traced)
    resolve(log_dir, "plan-check")
    ranged = [statement for statement in statements if "FROM seen" in statement and "id>=" in statement.replace(" ", "")]
    assert ranged and not any("LIKE" in statement.upper() for statement in statements)


# --- tasks: authority --------------------------------------------------------------------------


def test_a_managed_caller_resolves_only_within_its_authorized_ids(log_dir: Path) -> None:
    write_tasks(log_dir, "owned-task-1", "foreign-task-1", "owned-task-2")
    managed = node_view("owned-task-1")
    assert resolve(log_dir, "owned-task", managed=managed) == Resolution("owned-task-1", {"input": "owned-task", "task_id": "owned-task-1"})
    # Two owned-looking ids exist on disk, but only one is authorized: no ambiguity is reported.
    # An unauthorized prefix is returned unchanged for the guard to refuse, never "unknown".
    assert resolve(log_dir, "foreign-task", managed=managed) == Resolution("foreign-task", None)
    with pytest.raises(ValueError):
        workflow_inspection.guard_task_read("foreign-task", managed)


def test_a_managed_caller_is_told_nothing_about_missing_ids(log_dir: Path) -> None:
    managed = node_view("owned-task-1")
    for value in ("no-such-task-anywhere", UUID_A, "short"):
        assert resolve(log_dir, value, managed=managed) == Resolution(value, None)


def test_a_managed_exact_id_is_not_rewritten(log_dir: Path) -> None:
    write_tasks(log_dir, "owned-task-1", "owned-task-12")
    managed = node_view("owned-task-1", "owned-task-12")
    assert resolve(log_dir, "owned-task-1", managed=managed) == Resolution("owned-task-1", None)


def test_a_managed_caller_sees_ambiguity_only_among_its_own_ids(log_dir: Path) -> None:
    managed = node_view("owned-task-1", "owned-task-2")
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "owned-task", managed=managed)
    assert [item["task_id"] for item in caught.value.data["candidates"]] == ["owned-task-1", "owned-task-2"]


def test_allowed_task_ids_matches_what_the_guard_accepts() -> None:
    association = {"role": "node", "activation_id": "exec"}
    run = {"workflow_run_id": "run", "activations": [
        {"id": "exec", "role": "node", "status": "running", "tasks": [{"task_id": "mine"}]},
        {"id": "other", "role": "node", "status": "running", "tasks": [{"task_id": "theirs"}]}]}
    assert workflow_inspection.allowed_task_ids((association, run)) == {"mine"}


# --- workflow runs -----------------------------------------------------------------------------

RUN_A = "0123456789abcdef0123456789abcdef"
RUN_B = "0123456789abcdef9999999999999999"


def test_run_prefix_resolves_exact_wins_and_ambiguity_is_reported(root: Path) -> None:
    write_runs(root, RUN_A, RUN_B, "custom-run-id", "custom-run-id-2")
    assert id_resolution.resolve_run(root, "0123456789abcdef0") == Resolution(RUN_A, {"input": "0123456789abcdef0", "workflow_run_id": RUN_A})
    assert id_resolution.resolve_run(root, RUN_A) == Resolution(RUN_A, None)
    assert id_resolution.resolve_run(root, "custom-run-id") == Resolution("custom-run-id", None)
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_run(root, "0123456789abcdef")
    assert caught.value.code == "ambiguous_workflow_run_id"
    assert [item["workflow_run_id"] for item in caught.value.data["candidates"]] == [RUN_A, RUN_B]
    assert set(caught.value.data["candidates"][0]) == {"workflow_run_id", "name", "status", "created_at"}


def test_run_too_short_unknown_uuid_and_missing_runs(root: Path) -> None:
    write_runs(root, RUN_A)
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_run(root, RUN_A[:7])
    assert (caught.value.code, caught.value.data["reason"], caught.value.data["min_prefix_length"]) == ("unknown_workflow_run", "prefix_too_short", 8)
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_run(root, "f" * 32)
    assert caught.value.code == "unknown_workflow_run" and "reason" not in caught.value.data
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_run(root, "ffffffffffff")
    assert caught.value.data["reason"] == "no_prefix_match"
    with pytest.raises(ValueError):
        id_resolution.resolve_run(root, "../x")


def test_run_dots_are_part_of_the_charset(root: Path) -> None:
    write_runs(root, "run.with.dots.0001")
    assert id_resolution.resolve_run(root, "run.with.dots").id == "run.with.dots.0001"


def test_run_discovery_failure_is_not_guessed(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_runs(root, RUN_A)
    monkeypatch.setattr(catalog.Catalog, "_discover", lambda self, db: db.execute("DELETE FROM state WHERE key='complete'"))
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_run(root, RUN_A[:12])
    assert caught.value.data["reason"] == "prefix_index_unavailable"


def test_a_managed_run_resolves_its_own_run_only(root: Path) -> None:
    write_runs(root, "run-own-000000001", "run-own-000000002")
    managed = node_view()
    assert id_resolution.resolve_run(root, "run-own-0000", managed=managed).id == "run-own-000000001"
    assert id_resolution.resolve_run(root, "run-own-000000002", managed=managed) == Resolution("run-own-000000002", None)
    assert id_resolution.resolve_run(root, "ffffffffffff", managed=managed) == Resolution("ffffffffffff", None)


def test_a_managed_tree_scope_verifies_the_chain(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflows
    managed = node_view()
    monkeypatch.setattr(workflows.WorkflowStore, "tree_members", lambda self, run_id, limit=65: {"state": "ready", "members": [{"id": "run-own-000000001"}, {"id": "child-run-0000001"}]})
    verified = []
    monkeypatch.setattr(workflow_inspection, "_authorized_linked_run", lambda run, root, target: verified.append(target) or {})
    assert id_resolution.resolve_run(root, "child-run", managed=managed, scope="tree").id == "child-run-0000001"
    assert verified == ["child-run-0000001"]
    monkeypatch.setattr(workflow_inspection, "_authorized_linked_run", lambda run, root, target: (_ for _ in ()).throw(ValueError("not linked")))
    assert id_resolution.resolve_run(root, "child-run", managed=managed, scope="tree") == Resolution("child-run", None)


def test_a_preparing_tree_does_not_guess(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflows
    monkeypatch.setattr(workflows.WorkflowStore, "tree_members", lambda self, run_id, limit=65: {"state": "preparing", "members": []})
    assert id_resolution.resolve_run(root, "child-run-0000001", managed=node_view(), scope="tree") == Resolution("child-run-0000001", None)


def test_run_local_task_ids_resolve_against_the_run_activations() -> None:
    task_ids = {"native-exec-0001", "attempt-task-0002", "attempt-task-0003"}
    assert id_resolution.resolve_in_set(id_resolution.TASK, "native-exec", task_ids).id == "native-exec-0001"
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_in_set(id_resolution.TASK, "attempt-task", task_ids)
    assert caught.value.code == "ambiguous_task_id"
    assert id_resolution.resolve_in_set(id_resolution.TASK, "nothing-here-at-all", task_ids) == Resolution("nothing-here-at-all", None)
    assert id_resolution.run_task_ids({"activations": [{"tasks": [{"task_id": "a"}, {"task_id": "b"}]}, {"tasks": []}]}) == {"a", "b"}


def assigned_view():
    association = {"role": "node", "activation_id": "exec"}
    run = {"workflow_run_id": "run-own-000000001", "activations": [
        {"id": "exec", "role": "node", "status": "running", "authorized_input_refs": [
            {"workflow_run_id": "child-run-0000001", "execution_id": "e1", "task_id": "child-task-000001"},
            {"workflow_run_id": "child-run-0000001", "execution_id": "e1", "task_id": None},
            {"workflow_run_id": "child-run-0000002", "execution_id": "e1", "task_id": "child-task-000002"},
            {"workflow_run_id": "child-run-0000003", "execution_id": "other", "task_id": "child-task-000003"}]},
        {"id": "sibling", "role": "node", "status": "running", "authorized_input_refs": [
            {"workflow_run_id": "foreign-run-000001", "execution_id": "e1", "task_id": "foreign-task-00001"}]}]}
    return association, run


def test_assigned_input_references_resolve_jointly_against_the_caller_activation() -> None:
    resolved = id_resolution.resolve_assigned_input(assigned_view(), "run-own-0000", "e1", "child-run-0000001", "child-task-0000")
    assert resolved["workflow_run_id"].id == "run-own-000000001"
    assert resolved["source_run_id"] == Resolution("child-run-0000001", None)
    assert resolved["task_id"].id == "child-task-000001"
    assert resolved["task_id"].resolved_from == {"input": "child-task-0000", "task_id": "child-task-000001"}


def test_assigned_input_prefixes_never_mix_activations_executions_or_runs() -> None:
    # `child-task-0000` also prefixes tasks of another run and execution; only the source run's own refs count.
    view = assigned_view()
    resolved = id_resolution.resolve_assigned_input(view, "run-own-000000001", "e1", "child-run-0000002", "child-task")
    assert resolved["task_id"].id == "child-task-000002"
    assert resolved["source_run_id"] == Resolution("child-run-0000002", None)
    # A sibling activation's reference is not a candidate at all.
    assert id_resolution.resolve_assigned_input(view, "run-own-000000001", "e1", "foreign-run-000001", None)["source_run_id"] == Resolution("foreign-run-000001", None)


def test_assigned_input_ambiguity_is_reported_within_the_caller_activation() -> None:
    with pytest.raises(IdResolutionError) as caught:
        id_resolution.resolve_assigned_input(assigned_view(), "run-own-000000001", "e1", "child-run-00000", None)
    assert caught.value.code == "ambiguous_workflow_run_id"


def test_assigned_input_passes_non_workers_through_for_the_read_to_refuse() -> None:
    resolved = id_resolution.resolve_assigned_input(None, "run-own-0000", "e1", "child-run-0000001", "x")
    assert resolved["workflow_run_id"] == Resolution("run-own-0000", None)


def test_echo_adds_the_primary_and_argument_entries_only_for_prefixes() -> None:
    prefix = Resolution("full", {"input": "fu", "task_id": "full"})
    exact = Resolution("exact", None)
    assert id_resolution.echo({"a": 1}, primary=exact) == {"a": 1}
    assert id_resolution.echo({"a": 1}, primary=prefix, task_id=exact, run_ids=[prefix, exact]) == {
        "a": 1, "resolved_from": prefix.resolved_from, "resolved_from_run_ids": [prefix.resolved_from]}
    assert id_resolution.echo([{"a": 1}], primary=prefix) == [{"a": 1}]


# --- MCP boundary ------------------------------------------------------------------------------


def error_data(exc: MCPError) -> dict:
    return exc.error.data


async def test_get_task_status_resolves_a_prefix_and_echoes_it(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    reply = await server.get_task_status(UUID_A[:13])
    assert reply["task_id"] == UUID_A
    assert reply["resolved_from"] == {"input": UUID_A[:13], "task_id": UUID_A}
    assert "resolved_from" not in await server.get_task_status(UUID_A)


async def test_mcp_errors_carry_a_stable_code(log_dir: Path) -> None:
    write_tasks(log_dir, "shared-prefix-1", "shared-prefix-2")
    with pytest.raises(MCPError) as ambiguous:
        await server.get_task_status("shared-prefix")
    assert error_data(ambiguous.value)["code"] == "ambiguous_task_id"
    assert len(error_data(ambiguous.value)["candidates"]) == 2
    with pytest.raises(MCPError) as short:
        await server.get_task_status("shared")
    assert error_data(short.value) == {"code": "unknown_task", "input": "shared", "reason": "prefix_too_short", "min_prefix_length": 8}
    with pytest.raises(MCPError) as missing:
        await server.get_task_status(UUID_B)
    assert error_data(missing.value)["code"] == "unknown_task"
    with pytest.raises(MCPError) as invalid:
        await server.get_task_status("../escape")
    assert invalid.value.error.data is None


async def test_wait_for_tasks_refuses_two_prefixes_naming_one_task(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    with pytest.raises(MCPError, match="duplicate|unique"):
        await server.wait_for_tasks([UUID_A[:10], UUID_A[:12]], timeout_seconds=0)
    reply = await server.wait_for_tasks([UUID_A[:10]], timeout_seconds=0)
    assert reply["tasks"][0]["task_id"] == UUID_A
    assert reply["resolved_from_task_ids"] == [{"input": UUID_A[:10], "task_id": UUID_A}]


async def test_task_page_resolves_listed_ids_and_still_drops_missing_canonical_ids(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    page = await server.list_task_page(task_ids=[UUID_A[:12], UUID_B])
    assert [item["task_id"] for item in page["items"]] == [UUID_A]
    assert page["resolved_from_task_ids"] == [{"input": UUID_A[:12], "task_id": UUID_A}]
    with pytest.raises(MCPError) as caught:
        await server.list_task_page(task_ids=["nomatchprefix"])
    assert error_data(caught.value)["code"] == "unknown_task"


async def test_predecessor_prefix_resolves(log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    store.write(log_dir, make_record(UUID_B, continuation_of_task_id=UUID_A))
    page = await server.list_task_page(predecessor_task_id=UUID_A[:12])
    assert [item["task_id"] for item in page["items"]] == [UUID_B]
    assert page["resolved_from_predecessor_task_id"] == {"input": UUID_A[:12], "task_id": UUID_A}


async def test_event_tools_echo_and_rebind(tmp_path: Path, log_dir: Path) -> None:
    write_tasks(log_dir, UUID_A)
    with state_ownership.state_session(resolve_layout(root=tmp_path / "state")):
        events = await server.get_task_events(UUID_A[:12])
        assert events["task_id"] == UUID_A and events["resolved_from"]["task_id"] == UUID_A
        page = await server.get_task_event_page(UUID_A[:12])
        assert page["task_id"] == UUID_A and page["resolved_from"]["input"] == UUID_A[:12]


async def test_task_artifacts_resolve_for_a_direct_caller(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import artifacts
    write_tasks(log_dir, UUID_A)
    seen = []
    monkeypatch.setattr(artifacts, "_caller", lambda directory: None)
    monkeypatch.setattr(artifacts, "_get_page", lambda directory, task_id, cursor, limit, caller: seen.append(task_id) or {"task_id": task_id})
    reply = await server.get_task_artifacts(UUID_A[:12])
    assert seen == [UUID_A] and reply["resolved_from"] == {"input": UUID_A[:12], "task_id": UUID_A}


async def test_workflow_run_tools_resolve_runs(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_runs(root, RUN_A)
    seen = []

    async def fake_dispatch(action, **kwargs):
        seen.append((action, kwargs["run_id"]))
        return {"workflow_run_id": kwargs["run_id"], "status": "paused"}

    monkeypatch.setattr(server, "_workflow_dispatch", fake_dispatch)
    reply = await server._workflow_call("pause", run_id=RUN_A[:10])
    assert seen == [("pause", RUN_A)]
    assert reply["resolved_from"] == {"input": RUN_A[:10], "workflow_run_id": RUN_A}
    with pytest.raises(IdResolutionError) as caught:
        await server._workflow_call("pause", run_id="ffffffffffff")
    assert caught.value.code == "unknown_workflow_run"
    # Creation has no run to resolve: a call without a run id is dispatched untouched.
    seen.clear()

    async def creation(action, **kwargs):
        seen.append((action, kwargs))
        return {}

    monkeypatch.setattr(server, "_workflow_dispatch", creation)
    await server._workflow_call("start", name="x", prompt="p", repo_path="/r")
    assert seen == [("start", {"name": "x", "prompt": "p", "repo_path": "/r"})]


async def test_unknown_workflow_run_is_an_mcp_error_with_a_code(root: Path) -> None:
    write_runs(root, RUN_A, RUN_B)
    for call, kwargs in ((server.pause_workflow, {"workflow_run_id": "0123456789abcdef"}), (server.get_workflow_status, {"workflow_run_id": "0123456789abcdef"})):
        with pytest.raises(MCPError) as caught:
            await call(**kwargs)
        assert error_data(caught.value)["code"] == "ambiguous_workflow_run_id"
    with pytest.raises(MCPError) as caught:
        await server.get_workflow_status("f" * 32)
    assert error_data(caught.value)["code"] == "unknown_workflow_run"


# --- ctl boundary ------------------------------------------------------------------------------


@pytest.fixture
def home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path / "ctl-home"))
    (tmp_path / "ctl-home").mkdir()
    return tmp_path / "ctl-home"


def test_ctl_status_resolves_a_prefix(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    assert ctl.main(["status", UUID_A[:12], "--json"]) == 0
    task = json.loads(capsys.readouterr().out)["task"]
    assert task["task_id"] == UUID_A
    assert task["resolved_from"] == {"input": UUID_A[:12], "task_id": UUID_A}


def test_ctl_status_errors_keep_their_code_and_gain_data(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), "shared-prefix-1", "shared-prefix-2")
    assert ctl.main(["status", "shared-prefix", "--json"]) == 1
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "ambiguous_task_id" and error["data"]["truncated"] is False
    assert ctl.main(["status", "shared", "--json"]) == 1
    error = json.loads(capsys.readouterr().out)["error"]
    assert (error["code"], error["data"]["reason"]) == ("unknown_task", "prefix_too_short")


def test_status_batch_keeps_one_row_per_input_and_errors_keyed_by_input(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A, "shared-prefix-1", "shared-prefix-2")
    assert ctl.main(["status-batch", "--task-ids", f"{UUID_A[:10]},{UUID_A[:12]},{UUID_A},shared-prefix,short,{UUID_B}", "--json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    assert [task["task_id"] for task in doc["tasks"]] == [UUID_A, UUID_A, UUID_A]
    assert [task.get("resolved_from") for task in doc["tasks"]] == [
        {"input": UUID_A[:10], "task_id": UUID_A}, {"input": UUID_A[:12], "task_id": UUID_A}, None]
    assert doc["errors"]["shared-prefix"]["code"] == "ambiguous_task_id"
    assert doc["errors"]["short"]["data"]["reason"] == "prefix_too_short"
    assert doc["errors"][UUID_B]["code"] == "unknown_task"


def test_ctl_task_list_page_resolves_listed_ids(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    assert ctl.main(["task-list-page", "--task-ids", f"{UUID_A[:12]},{UUID_B}", "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert [item["task_id"] for item in result["items"]] == [UUID_A]
    assert result["resolved_from_task_ids"] == [{"input": UUID_A[:12], "task_id": UUID_A}]


def test_ctl_cancel_by_prefix_resolves_before_acting(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    assert ctl.main(["cancel", UUID_A[:12], "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["task_id"] == UUID_A and result["resolved_from"]["input"] == UUID_A[:12]


def test_ctl_unknown_workflow_run_replaces_workflow_error(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    for argv in (["workflow-status", "f" * 32], ["workflow-pause", "f" * 32], ["workflow-detail", "f" * 32, "--view", "status"]):
        assert ctl.main([*argv, "--json"]) == 1
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "unknown_workflow_run"


def test_ctl_workflow_notifications_resolve_the_run(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    write_runs(ctl_log_dir().parent, RUN_A, RUN_B)
    assert ctl.main(["workflow-notifications", "0123456789abcdef", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "ambiguous_workflow_run_id"


# --- every id parameter is wired ---------------------------------------------------------------

# Every MCP tool parameter and ctl argument that names a task or a workflow run, with how it is resolved.
# A new id parameter fails this test until it is wired through id_resolution or exempted with a reason.
ID_PARAMETER = re.compile(r"(^|_)(task_ids?|run_ids?|source_run_id|related_run_id|workflow_run_id|predecessor_task_id)$")

MCP_RESOLVED = {
    ("get_task_status", "task_id"), ("wait_for_task", "task_id"), ("wait_for_tasks", "task_ids"),
    ("continue_task", "task_id"), ("resume_task", "task_id"), ("cancel_task", "task_id"),
    ("get_task_events", "task_id"), ("get_task_event_page", "task_id"), ("send_message", "task_id"),
    ("cancel_queued_message", "task_id"), ("subscribe", "task_id"), ("unsubscribe", "task_id"),
    ("get_task_notifications", "task_id"), ("list_approval_requests", "task_id"),
    ("list_approval_requests", "workflow_run_id"), ("get_approval_request", "task_id"),
    ("respond_approval_request", "task_id"), ("get_task_artifacts", "task_id"),
    ("list_task_page", "task_ids"), ("list_task_page", "predecessor_task_id"),
    ("followup_workflow_builder", "run_id"), ("subscribe_workflow", "workflow_run_id"),
    ("unsubscribe_workflow", "workflow_run_id"), ("get_workflow_notifications", "workflow_run_id"),
    ("revise_workflow_goal", "run_id"), ("list_workflow_run_page", "related_run_id"),
    ("list_workflow_run_page", "run_ids"), ("get_workflow_status", "workflow_run_id"),
    ("wait_for_workflow", "workflow_run_id"), ("get_workflow_run_detail", "workflow_run_id"),
    ("pause_workflow", "workflow_run_id"), ("resume_workflow", "workflow_run_id"),
    ("recover_workflow", "workflow_run_id"), ("cancel_workflow", "workflow_run_id"),
    ("read_workflow_assigned_input", "workflow_run_id"), ("read_workflow_assigned_input", "source_run_id"),
    ("read_workflow_assigned_input", "task_id"), ("inspect_workflow_node", "workflow_run_id"),
    ("inspect_workflow_node", "task_id"), ("send_workflow_message", "run_id"), ("send_workflow_message", "task_id"),
    ("cancel_workflow_message", "run_id"), ("get_workflow_artifacts", "workflow_run_id"),
    ("get_workflow_artifacts", "task_id"),
}
MCP_EXEMPT: dict[tuple[str, str], str] = {}

CTL_RESOLVED = {
    ("approval-get", "task_id"), ("approval-list", "--task-id"), ("approval-list", "--workflow-run-id"),
    ("approval-respond", "task_id"), ("cancel", "task_id"), ("cancel-message", "task_id"), ("continue", "task_id"),
    ("launch-settle", "--task-id"), ("recovery-activity", "task_id"), ("resume", "task_id"), ("send", "task_id"),
    ("status", "task_id"), ("status-batch", "--task-ids"), ("takeover", "task_id"), ("takeover-attach", "task_id"),
    ("task-artifacts", "task_id"), ("task-list-page", "--task-ids"),
    ("workflow-abandon-dispatch", "task_id"), ("workflow-abandon-dispatch", "workflow_run_id"),
    ("workflow-artifacts", "--task-id"), ("workflow-artifacts", "workflow_run_id"),
    ("workflow-assigned-input", "--source-run"), ("workflow-assigned-input", "--task-id"),
    ("workflow-assigned-input", "workflow_run_id"), ("workflow-builder-followup", "workflow_run_id"),
    ("workflow-cancel", "workflow_run_id"), ("workflow-cancel-message", "workflow_run_id"),
    ("workflow-detail", "workflow_run_id"), ("workflow-inspect", "--task-id"), ("workflow-inspect", "workflow_run_id"),
    ("workflow-list-page", "--related-run-id"), ("workflow-list-page", "--run-ids"),
    ("workflow-notifications", "workflow_run_id"), ("workflow-pause", "workflow_run_id"),
    ("workflow-recover", "workflow_run_id"), ("workflow-resume", "workflow_run_id"),
    ("workflow-revise-goal", "workflow_run_id"), ("workflow-send", "--task-id"), ("workflow-send", "workflow_run_id"),
    ("workflow-status", "workflow_run_id"), ("workflow-wait", "workflow_run_id"),
}
CTL_EXEMPT = {
    ("worktree-remove", "--task-id"): "destructive and accepts a task or run id in one namespace; the full id is required",
}


async def test_every_mcp_id_parameter_is_wired() -> None:
    from mcp import Client
    async with Client(server.mcp) as client:
        tools = (await client.list_tools()).tools
    found = {(tool.name, name) for tool in tools for name in tool.input_schema.get("properties", {}) if ID_PARAMETER.search(name)}
    unwired = found - MCP_RESOLVED - set(MCP_EXEMPT)
    assert not unwired, f"id parameters without prefix resolution: {sorted(unwired)}"
    assert not (MCP_RESOLVED - found), f"wiring table names parameters that no longer exist: {sorted(MCP_RESOLVED - found)}"


def test_every_ctl_id_argument_is_wired() -> None:
    parser = ctl._build_parser()[0]
    subparsers = next(action for action in parser._actions if getattr(action, "choices", None))
    found = set()
    for command, sub in subparsers.choices.items():
        for action in sub._actions:
            label = next((option for option in action.option_strings if option.startswith("--")), action.dest)
            # Probe both spellings: `--source-run` has the dest `source_run_id`.
            if ID_PARAMETER.search(action.dest) or ID_PARAMETER.search(label.lstrip("-").replace("-", "_")):
                found.add((command, label))
    wired = CTL_RESOLVED | set(CTL_EXEMPT)
    assert not found - wired, f"ctl id arguments without prefix resolution: {sorted(found - wired)}"
    assert not wired - found, f"wiring table names arguments that no longer exist: {sorted(wired - found)}"


def test_worktree_remove_requires_the_full_id(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    code = ctl.main(["worktree-remove", "--task-id", UUID_A[:12], "--json"])
    assert code == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] != "ambiguous_task_id"


# --- reads through real run documents ----------------------------------------------------------


def write_run_document(root: Path, run_id: str, *, status: str = "completed", sequence: int = 1) -> None:
    from harflow_mcp import workflow_run_documents
    runs = root / "workflow-runs"
    runs.mkdir(parents=True, exist_ok=True)
    run = {"workflow_run_id": run_id, "kind": "workflow", "name": "Build", "status": status,
           "sequence": sequence, "revision": 0, "transitions": 0, "created_at": float(sequence),
           "updated_at": float(sequence), "prompt": "", "repo_path": str(root), "freedom": "write_in_repo",
           "network": None, "instructions": "", "permission_policy": "legacy_ceiling", "definition_hash": "hash",
           "definition": {"name": "Build", "workflow_id": "wf", "revision": 1, "orchestrator": {"backend": "codex"}, "nodes": [], "connections": []},
           "activations": [], "decisions": [], "tasks": [], "sessions": {}, "pending": [], "joins": {},
           "attempt_grants": {}, "retry_counts": {}, "retry_grants": {}, "suppressed_candidates": [],
           "supervisor_pid": None}
    (runs / f"{run_id}.json").write_text(json.dumps(workflow_run_documents.encode(run)))


async def test_workflow_status_by_prefix_reads_the_real_run(root: Path) -> None:
    write_run_document(root, RUN_A)
    reply = await server.get_workflow_status(RUN_A[:12])
    assert reply["workflow_run_id"] == RUN_A and reply["status"] == "completed"
    assert reply["resolved_from"] == {"input": RUN_A[:12], "workflow_run_id": RUN_A}
    assert "resolved_from" not in await server.get_workflow_status(RUN_A)


async def test_wait_for_workflow_by_prefix_observes_the_resolved_file(root: Path) -> None:
    write_run_document(root, RUN_A)
    reply = await server.wait_for_workflow(RUN_A[:12], timeout_seconds=0)
    assert reply["workflow_run_id"] == RUN_A
    assert reply["resolved_from"]["workflow_run_id"] == RUN_A
    assert reply["stop_reason"] == "terminal"


async def test_run_page_resolves_related_and_listed_ids(root: Path) -> None:
    write_run_document(root, RUN_A)
    page = await server.list_workflow_run_page(run_ids=[RUN_A[:12], "f" * 32])
    assert [item["workflow_run_id"] for item in page["items"]] == [RUN_A]
    assert page["resolved_from_run_ids"] == [{"input": RUN_A[:12], "workflow_run_id": RUN_A}]
    related = await server.list_workflow_run_page(related_run_id=RUN_A[:12])
    assert related["resolved_from_related_run_id"]["workflow_run_id"] == RUN_A


async def test_run_detail_by_prefix(root: Path) -> None:
    write_run_document(root, RUN_A)
    detail = await server.get_workflow_run_detail(RUN_A[:12], "definition")
    assert detail["resolved_from"]["workflow_run_id"] == RUN_A


def test_ctl_workflow_status_by_prefix(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    write_run_document(ctl_log_dir().parent, RUN_A)
    assert ctl.main(["workflow-status", RUN_A[:12], "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["workflow_run_id"] == RUN_A and result["resolved_from"]["input"] == RUN_A[:12]


async def test_workflow_message_tools_resolve_run_and_run_local_task(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflow_messages
    write_run_document(root, RUN_A)
    captured = []

    def fake_send(storage, run_id, text, *, task_id, delivery, interaction_owner):
        captured.append((run_id, task_id))
        return {"message_id": "m1", "status": "queued"}

    async def fake_deliver(storage, result, *, interaction_owner):
        return result

    monkeypatch.setattr(workflow_messages, "send", fake_send)
    monkeypatch.setattr(workflow_messages, "deliver_now", fake_deliver)
    reply = await server.send_workflow_message(RUN_A[:12], "hello")
    assert captured == [(RUN_A, None)]
    assert reply["resolved_from"] == {"input": RUN_A[:12], "workflow_run_id": RUN_A}


# --- managed callers through the MCP boundary --------------------------------------------------


@pytest.fixture
def managed_node(monkeypatch: pytest.MonkeyPatch):
    view = node_view("owned-task-1")

    async def reader():
        return view

    monkeypatch.setattr(server, "_managed_workflow_reader", reader)
    return view


async def test_a_managed_node_resolves_its_own_task_by_prefix(log_dir: Path, managed_node) -> None:
    write_tasks(log_dir, "owned-task-1", "foreign-task-1")
    reply = await server.get_task_status("owned-task")
    assert reply["task_id"] == "owned-task-1"
    assert reply["resolved_from"] == {"input": "owned-task", "task_id": "owned-task-1"}


async def test_a_managed_node_gets_the_existing_refusal_for_other_tasks(log_dir: Path, managed_node) -> None:
    write_tasks(log_dir, "owned-task-1", "foreign-task-1")
    for value in ("foreign-task-1", "foreign-task", "no-such-task-at-all", UUID_A):
        with pytest.raises(MCPError) as caught:
            await server.get_task_status(value)
        assert "may only read tasks from their own workflow run" in str(caught.value) or "own execution" in str(caught.value)
        assert caught.value.error.data is None


async def test_unauthorized_ids_do_not_make_a_managed_prefix_ambiguous(log_dir: Path, managed_node) -> None:
    write_tasks(log_dir, "owned-task-1", "owned-task-2")
    reply = await server.get_task_status("owned-task")
    assert reply["task_id"] == "owned-task-1"


async def test_managed_task_pages_resolve_within_authority(log_dir: Path, managed_node, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflow_inspection
    write_tasks(log_dir, "owned-task-1", "foreign-task-1")
    monkeypatch.setattr(workflow_inspection, "managed_page_reader", lambda directory: (True, managed_node))
    page = await server.list_task_page(task_ids=["owned-task", "foreign-task-1"])
    assert [item["task_id"] for item in page["items"]] == ["owned-task-1"]


# --- review round 1 ----------------------------------------------------------------------------


def test_a_full_window_that_lost_names_is_queried_again(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ids = [f"window-prefix-{number}" for number in range(7)]
    write_tasks(log_dir, *ids)
    original = catalog.Catalog.prefix_names
    calls = []

    def delete_after_first(self, prefix, limit):
        found = original(self, prefix, limit)
        if not calls:
            for name in ids[:5]:
                store.record_path(log_dir, name).unlink()
        calls.append(1)
        return found

    monkeypatch.setattr(catalog.Catalog, "prefix_names", delete_after_first)
    # The first window held 6 names of which only the sixth survived; the seventh is also live.
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "window-prefix")
    assert caught.value.code == "ambiguous_task_id"
    assert sorted(item["task_id"] for item in caught.value.data["candidates"]) == ids[5:]
    assert len(calls) == 2


def test_a_window_that_keeps_losing_names_is_never_guessed(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tasks(log_dir, "window-prefix-live")
    names = [f"window-prefix-gone-{number}" for number in range(MAX_CANDIDATES)] + ["window-prefix-live"]
    monkeypatch.setattr(catalog.Catalog, "prefix_names", lambda self, prefix, limit: {"names": names, "headers": {}})
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, "window-prefix")
    assert caught.value.data["reason"] == "prefix_index_unavailable"


@pytest.mark.parametrize("failure", [sqlite3.OperationalError("database is locked"), OSError("disk error")])
def test_derivative_catalog_failures_report_the_index_as_unavailable(log_dir: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception) -> None:
    write_tasks(log_dir, UUID_A)

    def broken(self, prefix, limit):
        raise failure

    monkeypatch.setattr(catalog.Catalog, "prefix_names", broken)
    with pytest.raises(IdResolutionError) as caught:
        resolve(log_dir, UUID_A[:12])
    assert (caught.value.code, caught.value.data["reason"]) == ("unknown_task", "prefix_index_unavailable")


def test_authoritative_state_errors_still_propagate(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    write_tasks(log_dir, UUID_A)

    def refused(self, prefix, limit):
        raise state_ownership.StateIncompatible("incompatible")

    monkeypatch.setattr(catalog.Catalog, "prefix_names", refused)
    with pytest.raises(state_ownership.StateIncompatible):
        resolve(log_dir, UUID_A[:12])


def test_a_truncated_tree_is_not_a_namespace(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflows
    members = [{"id": "run-own-000000001"}] + [{"id": f"child-run-{number:07}"} for number in range(64)]
    monkeypatch.setattr(workflows.WorkflowStore, "tree_members", lambda self, run_id, limit=65: {"state": "ready", "members": members})
    monkeypatch.setattr(workflow_inspection, "_authorized_linked_run", lambda run, root, target: {})
    assert id_resolution.resolve_run(root, "child-run-0000001", managed=node_view(), scope="tree") == Resolution("child-run-0000001", None)
    assert id_resolution.resolve_run(root, "child-run-000", managed=node_view(), scope="tree") == Resolution("child-run-000", None)


def test_run_task_resolution_uses_a_bounded_read(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflows
    seen = []
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda self, run_id, **kwargs: seen.append(kwargs) or {"activations": [{"tasks": [{"task_id": "attempt-task-0001"}]}]})
    assert id_resolution.resolve_run_task(root, RUN_A, "attempt-task").id == "attempt-task-0001"
    assert seen and seen[0]["metadata_byte_limit"] == id_resolution.RUN_READ_BYTES


def test_an_unreadable_run_still_reaches_the_artifact_page(root: Path, log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import artifacts, workflow_node_reads, workflows
    write_runs(root, RUN_A)
    monkeypatch.setattr(workflows.WorkflowStore, "get_run", lambda self, run_id, **kwargs: (_ for _ in ()).throw(OSError("unreadable")))
    seen = []
    monkeypatch.setattr(artifacts, "workflow_page", lambda directory, run_id, **kwargs: seen.append((run_id, kwargs["task_id"])) or {"items": []})
    workflow_node_reads.workflow_artifacts_page(None, True, log_dir, RUN_A[:12], task_id="attempt-task-0001")
    assert seen == [(RUN_A, "attempt-task-0001")]


# Authority before resolution: a caller that may not act learns nothing about which ids exist.


def test_ctl_takeover_refuses_agents_before_resolving(home: Path, capsys: pytest.CaptureFixture, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import lineage
    write_tasks(ctl_log_dir(), "shared-prefix-1", "shared-prefix-2")
    monkeypatch.setenv(lineage.ENV_TASK_ID, "some-agent")
    for argv in (["takeover", "shared-prefix"], ["takeover-attach", "shared-prefix", "--pid", "1"]):
        assert ctl.main([*argv, "--json"]) == 1
        error = json.loads(capsys.readouterr().out)["error"]
        assert error["code"] in {"agent_caller", "caller_undecidable"}  # refused before any id lookup
        assert "data" not in error


async def test_workflow_mutations_refuse_a_managed_caller_before_resolving(root: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import workflow_hooks
    write_runs(root, RUN_A, RUN_B)
    caller = type("Caller", (), {"record": type("Record", (), {"task_id": "agent-task"})()})()

    async def verified():
        return caller

    def refuse(log_dir, task_id):
        raise ValueError("managed callers cannot control workflows")

    monkeypatch.setattr(server, "_verified_workflow_caller", verified)
    monkeypatch.setattr(workflow_hooks, "refuse_managed", refuse)
    for prefix in ("0123456789abcdef", "ffffffffffff", "short"):
        with pytest.raises(MCPError, match="managed callers cannot control workflows") as caught:
            await server._workflow_call("pause", run_id=prefix)
        assert caught.value.error.data is None


# Surfaces with their own missing-id contract keep it.


def test_ctl_approvals_keep_their_missing_id_contract(home: Path, log_dir: Path, capsys: pytest.CaptureFixture) -> None:
    # The CLI approval commands use the process registry, which the suite points at the isolated state.
    write_tasks(log_dir, "shared-prefix-1", "shared-prefix-2")
    assert ctl.main(["approval-list", "--task-id", UUID_B, "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["result"]["requests"] == []
    # A missing run was always an `approval_error` (its run file is unreadable); that is preserved.
    assert ctl.main(["approval-list", "--workflow-run-id", "f" * 32, "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "approval_error"
    for missing in (UUID_B, "t1"):
        assert ctl.main(["approval-get", missing, "request-1", "--json"]) == 1
        assert json.loads(capsys.readouterr().out)["error"]["code"] == "approval_error"
    assert ctl.main(["approval-get", "shared-prefix", "request-1", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "ambiguous_task_id"
    assert ctl.main(["approval-get", "nothing-matches-this", "request-1", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["error"]["code"] == "unknown_task"


async def test_mcp_approvals_pass_a_missing_id_through(log_dir: Path) -> None:
    write_tasks(log_dir, "other-task")
    assert (await server.list_approval_requests(task_id=UUID_B))["requests"] == []
    with pytest.raises(MCPError) as caught:
        await server.get_approval_request(UUID_B, "request-1")
    assert caught.value.error.data is None or caught.value.error.data.get("code") != "unknown_task"


def test_ctl_workflow_notifications_for_an_absent_run_is_empty(home: Path, capsys: pytest.CaptureFixture) -> None:
    write_tasks(ctl_log_dir(), UUID_A)
    assert ctl.main(["workflow-notifications", "f" * 32, "--json"]) == 0
    result = json.loads(capsys.readouterr().out)["result"]
    assert result["subscriptions"] == [] and result["deliveries"] == []


async def test_task_artifacts_keep_their_missing_id_error(log_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from harflow_mcp import artifacts
    write_tasks(log_dir, UUID_A)
    monkeypatch.setattr(artifacts, "_caller", lambda directory: None)
    for missing in (UUID_B, "t1"):
        with pytest.raises(MCPError) as caught:
            await server.get_task_artifacts(missing)
        assert str(caught.value) == "Unknown task"
    with pytest.raises(MCPError) as caught:
        await server.get_task_artifacts("nothing-matches-this")
    assert error_data(caught.value)["code"] == "unknown_task"


def test_a_corrupt_run_document_reaches_the_artifact_page(root: Path, log_dir: Path) -> None:
    from harflow_mcp import workflow_node_reads
    write_runs(root, RUN_A)
    (root / "workflow-runs" / f"{RUN_A}.json").write_text("not json")
    # The real store raises WorkflowRunDocumentError while resolving the task prefix; the page,
    # not the resolver, reports the unreadable run.
    page = workflow_node_reads.workflow_artifacts_page(None, True, log_dir, RUN_A[:12], task_id="attempt-task-0001")
    assert isinstance(page, dict) and page["resolved_from"]["workflow_run_id"] == RUN_A
    assert "resolved_from_task_id" not in page
    assert page.get("state") == "unavailable" or any(owner.get("state") == "run_unavailable" or owner.get("reason") == "run_unavailable" for owner in page.get("owners", []))
