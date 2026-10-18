import json
import logging

import pytest

from polybridge import wait_policy, workflow_notification_points as points
from polybridge import workflow_responses as responses
from polybridge import workflows as w


def definition():
    return {"name": "example", "orchestrator": {"backend": "codex"}, "nodes": [{"id": "start", "type": "start"}, {"id": "work", "type": "agent", "role": "task", "agent": {"backend": "codex"}, "instructions": "Work"}, {"id": "end", "type": "end"}], "connections": [{"id": "begin", "source": "start", "target": "work"}, {"id": "finish", "source": "work", "target": "end"}]}


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda b: True)
    return w.WorkflowStore(tmp_path)


@pytest.fixture
def run_id(store, tmp_path):
    return store.create_run(w.validate_definition(definition()), "build it", tmp_path)["workflow_run_id"]


def set_status(store, run_id, status, event="test", **fields):
    def mutate(run):
        run["status"] = status
        run.update(fields)
    return store.update_run(run_id, mutate, event)


def on_disk(store, run_id):
    return json.loads((store.runs / f"{run_id}.json").read_text())


def task(status):
    return {"activations": [{"tasks": [{"status": status}]}]}


# --- wait_policy.workflow_phase ---------------------------------------------------------------

@pytest.mark.parametrize("status", ["completed", "failed", "cancelled"])
def test_phase_terminal_statuses(status):
    assert wait_policy.workflow_phase(status, False) == "terminal"
    assert wait_policy.workflow_phase(status, True) == "settling"
    assert wait_policy.workflow_phase(status, False, True) == "settling"
    assert wait_policy.workflow_phase(status, True, True) == "settling"


@pytest.mark.parametrize("status", ["running", "pending", "building", "pausing", "starting", "cancelling"])
def test_phase_active_statuses_ignore_settling(status):
    assert wait_policy.workflow_phase(status, False) == "active"
    assert wait_policy.workflow_phase(status, True, True) == "active"


@pytest.mark.parametrize("status", ["needs_input", "needs_attention", "paused", "mystery", None])
def test_phase_attention_and_unknown_statuses(status):
    assert wait_policy.workflow_phase(status, False) == "attention"
    assert wait_policy.workflow_phase(status, True) == "attention"


# --- point_key ----------------------------------------------------------------------------------

def root(**fields):
    return {"workflow_run_id": "root", "sequence": 5, "activations": [], **fields}


def test_key_completed_and_cancelled():
    assert points.point_key(root(status="completed")) == "terminal:completed"
    assert points.point_key(root(status="cancelled")) == "terminal:cancelled"


def test_key_failed_uses_failed_since_sequence_and_ignores_reason():
    first = points.point_key(root(status="failed", failed_since_sequence=3, failure_reason="a", sequence=4))
    edited = points.point_key(root(status="failed", failed_since_sequence=3, failure_reason="b", sequence=9, attention_checkpoint=8))
    assert first == edited == "terminal:failed:3"


def test_key_failed_legacy_fallbacks():
    assert points.point_key(root(status="failed", attention_checkpoint=2)) == "terminal:failed:2"
    assert points.point_key(root(status="failed")) == "terminal:failed:5"


def test_key_needs_input_includes_decision_and_source():
    base = root(status="needs_input", input_decision_id="d1")
    assert points.point_key(base) == "attention:needs_input:d1:root"
    assert points.point_key({**base, "input_source": {"workflow_run_id": "child"}}) == "attention:needs_input:d1:child"
    assert points.point_key({**base, "input_decision_id": "d2"}) != points.point_key(base)


def test_key_nested_attention_differs_by_source_checkpoint():
    def key(cp):
        return points.point_key(root(status="needs_attention", attention_checkpoint=4, attention_reason="same",
                                     attention_source={"workflow_run_id": "c", "attention_checkpoint": cp}))
    assert key(1) != key(2)
    assert key(1) == "attention:needs_attention:4:c:1"


def test_key_caller_pause_is_none_but_system_pause_notifies():
    assert points.point_key(root(status="paused", pause_origin="caller", attention_checkpoint=2)) is None
    assert points.point_key(root(status="paused", attention_checkpoint=2), "control:pause") is None
    assert points.point_key(root(status="paused", attention_checkpoint=2), "task_state") == "attention:paused:2:root:None"


@pytest.mark.parametrize("fields", [
    {"status": "running"},
    {"status": "completed", **task("running")},
    {"status": "failed", **task("reserved")},
    {"status": "cancelled", **task("uncertain")},
    {"status": "completed", "parent_link": {"root_workflow_run_id": "r"}},
    {"status": "completed", "kind": "builder"},
    {"status": "needs_input", "kind": "builder", "input_decision_id": "d"},
])
def test_key_none_for_active_settling_child_and_builder(fields):
    assert points.point_key(root(**fields)) is None


def test_event_for():
    assert points.event_for("terminal:completed") == points.SETTLED
    assert points.event_for("attention:paused:1:x:None") == points.ATTENTION


# --- through WorkflowStore.update_run -----------------------------------------------------------

def test_terminal_point_persisted_atomically_without_settling(store, run_id):
    result = set_status(store, run_id, "completed", summary="done")
    disk = on_disk(store, run_id)
    assert "settling" not in disk and result["settling"] is False
    [point] = disk["terminal_notification_points"]
    assert point["key"] == "terminal:completed" and point["event"] == "workflow.settled"
    assert point["sequence"] == disk["sequence"] == result["sequence"]
    assert point["status"] == "completed" and point["observed_at"] == disk["updated_at"]
    assert point["snapshot"]["result"]["summary"] == "done"
    assert "notification_points" not in disk


def test_settled_point_appears_when_settling_ends(store, run_id):
    def start(run):
        run["status"] = "completed"
        run["activations"] = [{"id": "a", "tasks": [{"task_id": "t1", "status": "running"}]}]
    result = store.update_run(run_id, start, "test")
    assert result["settling"] is True
    assert "terminal_notification_points" not in on_disk(store, run_id)

    def settle(run):
        run["activations"][0]["tasks"][0]["status"] = "completed"
    result = store.update_run(run_id, settle, "task_state")
    assert result["settling"] is False
    [point] = on_disk(store, run_id)["terminal_notification_points"]
    assert point["key"] == "terminal:completed" and point["sequence"] == result["sequence"]
    assert "settling" not in on_disk(store, run_id)


def test_same_key_is_not_duplicated(store, run_id):
    set_status(store, run_id, "completed")
    set_status(store, run_id, "completed", summary="edit")
    assert len(on_disk(store, run_id)["terminal_notification_points"]) == 1


def test_failed_reason_edit_makes_no_new_point_and_recover_makes_one(store, run_id):
    set_status(store, run_id, "failed", failure_reason="a")
    first = on_disk(store, run_id)
    set_status(store, run_id, "failed", failure_reason="b")
    assert len(on_disk(store, run_id)["terminal_notification_points"]) == 1
    assert on_disk(store, run_id)["failed_since_sequence"] == first["failed_since_sequence"]
    set_status(store, run_id, "running")
    assert "failed_since_sequence" not in on_disk(store, run_id)
    set_status(store, run_id, "failed", failure_reason="b")
    keys = [p["key"] for p in on_disk(store, run_id)["terminal_notification_points"]]
    assert len(keys) == 2 and len(set(keys)) == 2


def test_attention_point_and_caller_pause_episode(store, run_id):
    set_status(store, run_id, "needs_input", input_decision_id="d1", input_question="why?")
    [point] = on_disk(store, run_id)["notification_points"]
    assert point["event"] == "workflow.attention"
    assert point["snapshot"]["attention"]["question"] == "why?"

    set_status(store, run_id, "paused", "control:pause", attention_reason="user")
    assert on_disk(store, run_id)["pause_origin"] == "caller"
    set_status(store, run_id, "paused", "task_state", attention_reason="other")
    assert on_disk(store, run_id)["pause_origin"] == "caller"
    assert len(on_disk(store, run_id)["notification_points"]) == 1

    set_status(store, run_id, "running")
    assert "pause_origin" not in on_disk(store, run_id)
    set_status(store, run_id, "paused", "task_state", attention_reason="system")
    disk = on_disk(store, run_id)
    assert "pause_origin" not in disk and len(disk["notification_points"]) == 2


def test_child_run_gets_no_points(store, run_id, tmp_path):
    child = store.create_run(w.validate_definition(definition()), "p", tmp_path)["workflow_run_id"]
    def link(run):
        run["parent_link"] = {"root_workflow_run_id": run_id, "workflow_run_id": run_id}
        run["status"] = "completed"
    store.update_run(child, link, "test")
    disk = on_disk(store, child)
    assert "terminal_notification_points" not in disk and "notification_points" not in disk
    assert "failed_since_sequence" not in disk


def test_caps_and_interleaved_watermarks(store, run_id):
    attention_seqs, terminal_seqs = [], []
    for i in range(70):
        if i % 2 == 0 and len(attention_seqs) < 40:
            r = set_status(store, run_id, "needs_attention", attention_reason=f"r{i}")
            attention_seqs.append(r["sequence"])
            set_status(store, run_id, "running")
        r = set_status(store, run_id, "failed", failure_reason="x")
        terminal_seqs.append(r["sequence"])
        set_status(store, run_id, "running")
    disk = on_disk(store, run_id)
    assert len(disk["terminal_notification_points"]) == points.TERMINAL_POINT_LIMIT
    assert len(disk["notification_points"]) == points.ATTENTION_POINT_LIMIT
    assert [p["sequence"] for p in disk["terminal_notification_points"]] == terminal_seqs[-64:]
    assert [p["sequence"] for p in disk["notification_points"]] == attention_seqs[-32:]
    assert disk["notification_evicted"] == {"terminal": terminal_seqs[5], "attention": attention_seqs[-33]}


def test_snapshot_failure_keeps_identity(store, run_id, monkeypatch):
    def boom(run):
        raise RuntimeError("no snapshot")
    monkeypatch.setattr(points, "bounded_snapshot", boom)
    set_status(store, run_id, "completed")
    [point] = on_disk(store, run_id)["terminal_notification_points"]
    assert point["key"] == "terminal:completed"
    assert point["snapshot"] is None and point["snapshot_error"] is True


def test_identity_failure_sets_error_and_mutation_succeeds(store, run_id, monkeypatch):
    def boom(run, event=None):
        raise RuntimeError("no key")
    monkeypatch.setattr(points, "point_key", boom)
    result = set_status(store, run_id, "completed")
    disk = on_disk(store, run_id)
    assert result["status"] == "completed" and disk["status"] == "completed"
    assert disk["notification_point_error"] == disk["sequence"]
    assert "terminal_notification_points" not in disk


def test_bookkeeping_exception_never_changes_outcome(store, run_id, monkeypatch):
    def boom(*args):
        raise RuntimeError("markers")
    monkeypatch.setattr(points, "update_markers", boom)
    assert set_status(store, run_id, "completed")["status"] == "completed"
    assert on_disk(store, run_id)["status"] == "completed"


def test_legacy_failed_run_snapshot_source():
    run = root(status="failed", updated_at=1.0)
    points.record_point(run, "test")
    [point] = run["terminal_notification_points"]
    assert point["snapshot_source"] == "legacy_state" and point["key"] == "terminal:failed:5"


# --- snapshot bounds ----------------------------------------------------------------------------

def test_snapshot_bounded_to_12_kib_and_question_capped():
    run = root(status="needs_input", input_decision_id="d", input_question="q" * 50_000, attention_reason="r" * 50_000,
               input_source={"workflow_run_id": "c" * 5000, "workflow_name": "n" * 5000, "path": ["p" * 5000] * 100})
    snapshot = points.bounded_snapshot(run)
    assert len(json.dumps(snapshot, ensure_ascii=False).encode()) <= points.SNAPSHOT_BYTE_LIMIT
    attention = snapshot["attention"]
    assert len(attention["question"]) <= 2000 and attention["question_truncated"] is True
    assert len(points.build_snapshot(run)["attention"]["question"]) == 2000


def test_snapshot_result_shape_for_terminal_run():
    run = root(status="failed", summary="s" * 10_000, failure_reason="f", checklist_disposition={"status": "x", "reason": "y" * 999})
    snapshot = points.bounded_snapshot(run)
    assert snapshot["attention"] is None
    assert len(snapshot["result"]["summary"]) <= points.SUMMARY_LIMIT
    assert len(snapshot["result"]["checklist_disposition"]["reason"]) == 256


def test_snapshot_over_bound_raises():
    run = root(status="completed", name="n" * 100_000)
    with pytest.raises(ValueError):
        points.bounded_snapshot(run)


# --- no leakage into views ----------------------------------------------------------------------

NEW_KEYS = {"notification_points", "terminal_notification_points", "notification_evicted",
            "notification_point_error", "failed_since_sequence", "pause_origin"}


def test_new_keys_do_not_leak_into_responses(store, run_id):
    set_status(store, run_id, "failed", failure_reason="x")
    set_status(store, run_id, "paused", "control:pause", attention_reason="p")
    set_status(store, run_id, "needs_input", input_decision_id="d")
    set_status(store, run_id, "completed")
    run = on_disk(store, run_id)
    run["notification_point_error"] = 1
    run["notification_evicted"] = {"terminal": 1}
    run["failed_since_sequence"] = 2
    run["pause_origin"] = "caller"
    assert NEW_KEYS & set(run)
    for view in (responses.compact(run), responses.monitor(run)):
        assert not NEW_KEYS & set(view)
        assert not any(k in json.dumps(view) for k in NEW_KEYS)
    for name in responses.VIEWS:
        chunk = responses.detail(run, name)["chunk"]
        assert not any(k in chunk for k in NEW_KEYS)


# --- requires_tree_check ------------------------------------------------------------------------

def invocation_activation(exec_id="x1", child_id="child", stage="running", status="completed"):
    return {"id": exec_id, "status": status, "tasks": [],
            "invocation": {"child_workflow_run_id": child_id, "stage": stage}}


def test_started_invocation_records_point_immediately_with_tree_check(store, run_id):
    set_status(store, run_id, "completed", activations=[invocation_activation()])
    [point] = on_disk(store, run_id)["terminal_notification_points"]
    assert point["key"] == "terminal:completed" and point["requires_tree_check"] is True
    assert point["snapshot"] is not None


def test_not_started_invocation_does_not_require_tree_check(store, run_id):
    set_status(store, run_id, "completed", activations=[invocation_activation(status="not_started")])
    [point] = on_disk(store, run_id)["terminal_notification_points"]
    assert "requires_tree_check" not in point


def test_no_invocation_does_not_require_tree_check(store, run_id):
    set_status(store, run_id, "completed")
    assert "requires_tree_check" not in on_disk(store, run_id)["terminal_notification_points"][0]


def test_failure_then_recovery_with_unsettled_child_keeps_failed_point(store, run_id):
    set_status(store, run_id, "failed", failure_reason="boom", activations=[invocation_activation()])
    set_status(store, run_id, "running")
    [point] = on_disk(store, run_id)["terminal_notification_points"]
    assert point["key"].startswith("terminal:failed:") and point["requires_tree_check"] is True
    assert point["snapshot"]["result"]["failure_reason"] == "boom"


# --- tree_settlement ----------------------------------------------------------------------------

NOW = 1000.0


def make_child(store, tmp_path, parent_id, exec_id, status="completed", activations=None, link=True):
    child_id = store.create_run(w.validate_definition(definition()), "c", tmp_path)["workflow_run_id"]
    def mutate(run):
        run["status"] = status
        if link:
            run["parent_link"] = {"workflow_run_id": parent_id, "root_workflow_run_id": parent_id, "execution_id": exec_id}
        run["activations"] = activations or []
    store.update_run(child_id, mutate, "test")
    return child_id


def root_with(store, run_id, activations, **fields):
    return set_status(store, run_id, "completed", activations=activations, **fields)


def settle(store, run, observed=NOW, now=NOW + 1, grace=600):
    return points.tree_settlement(store, run, now=now, grace_seconds=grace, observed_at=observed)


def test_settlement_settled_for_not_started_and_settled_trees(store, run_id, tmp_path):
    grandchild_holder = make_child(store, tmp_path, run_id, "x1")
    grand = make_child(store, tmp_path, grandchild_holder, "g1")
    def add(run):
        run["activations"] = [invocation_activation("g1", grand)]
    store.update_run(grandchild_holder, add, "test")
    run = root_with(store, run_id, [invocation_activation("x1", grandchild_holder),
                                    invocation_activation("x2", "missing", status="not_started")])
    assert settle(store, run) == "settled"


def test_settlement_pending_for_unsettled_child_within_grace(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1", status="running")
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert settle(store, run) == "pending"


def test_settlement_unsettled_grandchild_and_unsettled_tasks(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1", activations=[{"id": "t", "tasks": [{"task_id": "tk1", "status": "running"}]}])
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert settle(store, run) == "pending"


def test_settlement_unconfirmed_after_grace(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1", status="running")
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert settle(store, run, now=NOW + 601) == "unconfirmed"
    assert settle(store, run, now=NOW + 600) == "unconfirmed"


def test_settlement_grace_falls_back_to_updated_at(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1", status="running")
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert points.tree_settlement(store, run, now=run["updated_at"] + 1) == "pending"
    assert points.tree_settlement(store, run, now=run["updated_at"] + 601) == "unconfirmed"


@pytest.mark.parametrize("stage", ["running", "starting", "created"])
def test_settlement_missing_child_pending_at_non_preparing_stage(store, run_id, stage, monkeypatch):
    monkeypatch.setattr("polybridge.identity.identity_check", lambda rec: "dead")
    run = root_with(store, run_id, [invocation_activation("x1", "ghost", stage=stage)], supervisor_pid=None)
    assert settle(store, run) == "pending"


def test_settlement_missing_child_at_preparing_with_live_supervisor_is_pending(store, run_id, monkeypatch):
    monkeypatch.setattr("polybridge.identity.identity_check", lambda rec: "alive")
    monkeypatch.setattr(w, "_alive", lambda pid: True)
    for extra in ({"supervisor_identity": {"pid": 1}}, {"supervisor_pid": 4242}):
        run = root_with(store, run_id, [invocation_activation("x1", "ghost", stage="preparing")], **extra)
        assert settle(store, run) == "pending"


def test_settlement_missing_child_at_preparing_with_dead_supervisor_is_settled(store, run_id, monkeypatch):
    monkeypatch.setattr("polybridge.identity.identity_check", lambda rec: "dead")
    monkeypatch.setattr(w, "_alive", lambda pid: False)
    for extra in ({"supervisor_identity": {"pid": 1}}, {"supervisor_pid": 4242}, {"supervisor_pid": None}):
        run = root_with(store, run_id, [invocation_activation("x1", "ghost", stage="preparing")], **extra)
        run.pop("supervisor_identity", None) if "supervisor_identity" not in extra else None
        assert settle(store, run) == "settled"


def test_settlement_preparing_without_child_id_is_not_settled(store, run_id, monkeypatch):
    monkeypatch.setattr("polybridge.identity.identity_check", lambda rec: "dead")
    run = root_with(store, run_id, [invocation_activation("x1", None, stage="preparing")], supervisor_pid=None)
    assert settle(store, run) == "pending"


def test_settlement_parent_link_mismatch_not_settled(store, run_id, tmp_path):
    wrong_exec = make_child(store, tmp_path, run_id, "other")
    wrong_parent = make_child(store, tmp_path, "someone-else", "x2")
    for child, exec_id in ((wrong_exec, "x1"), (wrong_parent, "x2")):
        run = root_with(store, run_id, [invocation_activation(exec_id, child)])
        assert settle(store, run) == "pending"


def test_settlement_cycle_not_settled(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1")
    def self_invoke(run):
        run["activations"] = [invocation_activation("x1", child)]
    store.update_run(child, self_invoke, "test")
    child_run = store.get_run(child)
    # child invokes itself (parent link matches the child), which revisits it
    def relink(run):
        run["parent_link"] = {"workflow_run_id": child, "root_workflow_run_id": run_id, "execution_id": "x1"}
    store.update_run(child, relink, "test")
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert child_run["workflow_run_id"] == child
    assert settle(store, run) == "pending"


def test_settlement_more_than_64_runs_not_settled(store, run_id, tmp_path):
    def chain(length):
        parent, links = run_id, []
        for i in range(length):
            child = make_child(store, tmp_path, parent, f"e{i}")
            links.append((parent, child, f"e{i}"))
            parent = child
        for (_, child, _), (_, nxt, nxt_exec) in zip(links, links[1:]):
            def add(run, nxt=nxt, nxt_exec=nxt_exec):
                run["activations"] = [invocation_activation(nxt_exec, nxt)]
            store.update_run(child, add, "test")
        return links
    links = chain(66)
    run = root_with(store, run_id, [invocation_activation(links[0][2], links[0][1])])
    assert settle(store, run) == "pending"


def test_settlement_short_chain_is_settled(store, run_id, tmp_path):
    a = make_child(store, tmp_path, run_id, "e0")
    b = make_child(store, tmp_path, a, "e1")
    store.update_run(a, lambda r: r.update(activations=[invocation_activation("e1", b)]), "test")
    run = root_with(store, run_id, [invocation_activation("e0", a)])
    assert settle(store, run) == "settled"


def test_settlement_unreadable_child_is_not_settled(store, run_id, monkeypatch):
    def boom(child_id):
        raise OSError("unreadable")
    monkeypatch.setattr(store, "get_run", boom)
    run = root(status="completed", workflow_run_id=run_id, updated_at=NOW, activations=[invocation_activation("x1", "c")])
    assert settle(store, run) == "pending"


# --- _enforce_total_bound -----------------------------------------------------------------------

def sized(seq, key, size=100):
    return {"key": key, "sequence": seq, "snapshot": {"blob": "x" * size}}


def test_total_bound_nulls_oldest_first_and_spares_fresh(monkeypatch):
    monkeypatch.setattr(points, "SNAPSHOT_TOTAL_BYTE_LIMIT", 340)
    run = {"notification_points": [sized(2, "a2"), sized(5, "a5")],
           "terminal_notification_points": [sized(1, "t1"), sized(3, "t3"), sized(6, "t6")]}
    fresh = run["terminal_notification_points"][-1]
    points._enforce_total_bound(run, fresh)
    nulled = {p["key"] for n in run for p in run[n] if p["snapshot"] is None}
    assert nulled == {"t1", "a2"}
    for n in run:
        for p in run[n]:
            assert "key" in p and "sequence" in p
            assert p.get("snapshot_evicted") is bool(p["snapshot"] is None) or p["snapshot"] is not None


def test_total_bound_never_nulls_fresh_even_if_alone_over_limit(monkeypatch):
    monkeypatch.setattr(points, "SNAPSHOT_TOTAL_BYTE_LIMIT", 10)
    run = {"terminal_notification_points": [sized(1, "t1"), sized(9, "t9")]}
    points._enforce_total_bound(run, run["terminal_notification_points"][-1])
    assert run["terminal_notification_points"][0]["snapshot"] is None
    assert run["terminal_notification_points"][0]["snapshot_evicted"] is True
    assert run["terminal_notification_points"][1]["snapshot"] is not None


def test_total_bound_attention_before_terminal_at_equal_sequence(monkeypatch):
    monkeypatch.setattr(points, "SNAPSHOT_TOTAL_BYTE_LIMIT", 250)
    run = {"notification_points": [sized(4, "att")], "terminal_notification_points": [sized(4, "term"), sized(8, "new")]}
    points._enforce_total_bound(run, run["terminal_notification_points"][-1])
    assert run["notification_points"][0]["snapshot"] is None
    assert run["terminal_notification_points"][0]["snapshot"] is not None


def test_total_bound_noop_under_limit():
    run = {"notification_points": [sized(1, "a")]}
    points._enforce_total_bound(run, None)
    assert run["notification_points"][0]["snapshot"] is not None
    assert "snapshot_evicted" not in run["notification_points"][0]


# --- _TreeCheck: one rule at every depth, bounded reads -----------------------------------------

def test_cancelled_child_with_not_started_grandchild_is_settled(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1", status="cancelled",
                       activations=[invocation_activation("g1", "never-created", status="not_started")])
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert settle(store, run) == "settled"


def nested_missing_grandchild(store, run_id, tmp_path, **root_fields):
    child = make_child(store, tmp_path, run_id, "x1",
                       activations=[invocation_activation("g1", "ghost", stage="preparing")])
    return root_with(store, run_id, [invocation_activation("x1", child)], **root_fields)


def test_nested_grandchild_missing_at_preparing_settled_when_root_supervisor_gone(store, run_id, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_alive", lambda pid: False)
    run = nested_missing_grandchild(store, run_id, tmp_path, supervisor_pid=4242)
    assert settle(store, run) == "settled"


def test_nested_grandchild_missing_at_preparing_pending_when_root_supervisor_live(store, run_id, tmp_path, monkeypatch):
    monkeypatch.setattr(w, "_alive", lambda pid: True)
    run = nested_missing_grandchild(store, run_id, tmp_path, supervisor_pid=4242)
    assert settle(store, run) == "pending"
    assert settle(store, run, now=NOW + 601) == "unconfirmed"


def spy_get_run(store, monkeypatch):
    calls = []
    real = store.get_run
    def spy(run_id, **kwargs):
        calls.append((run_id, kwargs))
        return real(run_id, **kwargs)
    monkeypatch.setattr(store, "get_run", spy)
    return calls


def test_every_read_passes_limit_and_one_shared_budget(store, run_id, tmp_path, monkeypatch):
    a = make_child(store, tmp_path, run_id, "e0")
    b = make_child(store, tmp_path, a, "e1")
    store.update_run(a, lambda r: r.update(activations=[invocation_activation("e1", b)]), "test")
    run = root_with(store, run_id, [invocation_activation("e0", a)])
    calls = spy_get_run(store, monkeypatch)
    assert settle(store, run) == "settled"
    assert [c[0] for c in calls] == [a, b]
    for _, kwargs in calls:
        assert isinstance(kwargs.get("metadata_byte_limit"), int) and kwargs["metadata_byte_limit"] > 0
        assert kwargs.get("metadata_budget") is not None
    assert len({id(kwargs["metadata_budget"]) for _, kwargs in calls}) == 1


def test_oversized_child_record_is_unsettled_and_pending_in_grace(store, run_id, tmp_path, monkeypatch):
    child = make_child(store, tmp_path, run_id, "x1")
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    assert settle(store, run) == "settled"
    monkeypatch.setattr("polybridge.catalog.METADATA_BYTES", 64)
    assert settle(store, run) == "pending"
    assert settle(store, run, now=NOW + 601) == "unconfirmed"


def test_exhausted_budget_is_unsettled_and_pending_in_grace(store, run_id, tmp_path, monkeypatch):
    from polybridge import catalog
    real = catalog.MetadataBudget
    child = make_child(store, tmp_path, run_id, "x1")
    run = root_with(store, run_id, [invocation_activation("x1", child)])
    monkeypatch.setattr(catalog, "MetadataBudget", lambda: real(metadata_limit=16))
    assert settle(store, run) == "pending"
    assert settle(store, run, now=NOW + 601) == "unconfirmed"


def test_recovered_root_with_unwinding_child_keeps_failed_check_pending_then_unconfirmed(store, run_id, tmp_path):
    child = make_child(store, tmp_path, run_id, "x1", status="running")
    set_status(store, run_id, "failed", failure_reason="boom", activations=[invocation_activation("x1", child)])
    recovered = set_status(store, run_id, "running")
    disk = on_disk(store, run_id)
    [point] = disk["terminal_notification_points"]
    assert point["requires_tree_check"] is True and recovered["status"] == "running"
    observed = point["observed_at"]
    assert points.tree_settlement(store, recovered, now=observed + 10, observed_at=observed) == "pending"
    assert points.tree_settlement(store, recovered, now=observed + 601, observed_at=observed) == "unconfirmed"
