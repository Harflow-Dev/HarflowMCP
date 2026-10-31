"""Guarded recovery binds mutation authority to the checkpoint a caller observed."""
import copy
import json
import threading
import uuid

import pytest

from harflow_mcp import ctl, server, workflow_attention, workflow_run_documents, workflows
from test_workflow_run_node_recovery import store, write_run  # noqa: F401


def suspended(store, tmp_path, *, reason="The run needs review"):
    definition = store.save("checkpoint", {
        "nodes": [{"id": "start", "type": "start"}, {"id": "end", "type": "end"}],
        "connections": [{"id": "finish", "source": "start", "target": "end"}],
    })
    run = store.create_run(workflows.validate_definition(definition), "request", tmp_path)
    run_id = run["workflow_run_id"]
    return store.update_run(run_id, lambda value: value.update(status="needs_attention", attention_reason=reason), "suspend")


def record_bytes(storage, run_id):
    return (storage.runs / f"{run_id}.json").read_bytes()


def journal_bytes(storage, run_id):
    path = storage.runs / f"{run_id}.jsonl"
    return path.read_bytes() if path.exists() else b""


def test_checkpoint_is_stable_for_progress_rotates_for_semantic_change_and_survives_restart(store, tmp_path):
    # given
    run = suspended(store, tmp_path)
    first = workflow_attention.checkpoint_id(store, run)
    assert first and first.startswith("attention:")

    # when
    progressed = store.update_run(run["workflow_run_id"], lambda value: value.update(instructions="progress inside the issued attempt"), "progress")
    same = workflow_attention.checkpoint_id(store, progressed)
    changed = store.update_run(run["workflow_run_id"], lambda value: value.update(attention_reason="A different failure"), "new_reason")
    next_checkpoint = workflow_attention.checkpoint_id(store, changed)
    restarted = workflows.WorkflowStore(root=store.root)

    # then
    assert same == first
    assert next_checkpoint and next_checkpoint != first
    assert workflow_attention.checkpoint_id(restarted, restarted.get_run(run["workflow_run_id"])) == next_checkpoint


def test_stale_checkpoint_refuses_before_any_record_event_or_launch_changes(store, tmp_path, monkeypatch):
    # given
    run = suspended(store, tmp_path)
    stale = workflow_attention.checkpoint_id(store, run)
    assert stale
    store.update_run(run["workflow_run_id"], lambda value: value.update(attention_reason="New checkpoint"), "new_reason")
    before_record = record_bytes(store, run["workflow_run_id"])
    before_events = journal_bytes(store, run["workflow_run_id"])
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: pytest.fail("stale control must not request launch"))

    # when
    with pytest.raises(workflow_attention.CheckpointError) as raised:
        store.control(run["workflow_run_id"], "resume", "Do not apply this", 2,
                      attention_checkpoint_id=stale, interaction_owner="monitor")

    # then
    assert raised.value.code == "attention_checkpoint_stale"
    assert record_bytes(store, run["workflow_run_id"]) == before_record
    assert journal_bytes(store, run["workflow_run_id"]) == before_events


def test_current_checkpoint_allows_recovery_and_monitor_omission_fails_closed(store, tmp_path, monkeypatch):
    # given
    run = suspended(store, tmp_path)
    checkpoint = workflow_attention.checkpoint_id(store, run)
    assert checkpoint
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: None)

    # when
    with pytest.raises(workflow_attention.CheckpointError) as missing:
        store.control(run["workflow_run_id"], "resume", "Unbound answer", 1, interaction_owner="monitor")
    resumed = store.control(run["workflow_run_id"], "resume", "Bound answer", 1,
                            attention_checkpoint_id=checkpoint)

    # then
    assert missing.value.code == "attention_checkpoint_required"
    assert resumed["status"] == "running"
    assert resumed["instructions"] == "Bound answer"
    assert not resumed.get("retry_grants")


def test_monitor_question_snapshot_cannot_exempt_tokenless_attention_after_source_switch(store, tmp_path, monkeypatch):
    # given
    run = suspended(store, tmp_path, reason="Current attention B")
    run_id = run["workflow_run_id"]
    before_record = record_bytes(store, run_id)
    before_events = journal_bytes(store, run_id)
    original_get = store.get_run
    calls = 0

    def switched_snapshot(identifier, **options):
        nonlocal calls
        value = original_get(identifier, **options)
        calls += 1
        if calls == 1:
            value["status"] = "needs_input"  # The read snapshot was A; disk already contains attention B.
            value["input_decision_id"] = "question-A"
        return value

    monkeypatch.setattr(store, "get_run", switched_snapshot)
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: pytest.fail("tokenless request must not launch B"))

    # when
    with pytest.raises(workflow_attention.CheckpointError) as raised:
        store.control(run_id, "resume", "Old question prose", 2, interaction_owner="monitor")

    # then
    assert raised.value.code == "attention_checkpoint_required"
    assert record_bytes(store, run_id) == before_record
    assert journal_bytes(store, run_id) == before_events


def test_decision_id_does_not_fall_through_to_a_new_attention_checkpoint(store, tmp_path):
    # given
    run = suspended(store, tmp_path)
    before_record = record_bytes(store, run["workflow_run_id"])
    before_events = journal_bytes(store, run["workflow_run_id"])

    # when
    with pytest.raises(workflows.WorkflowError, match="decision_id.*not currently available"):
        store.control(run["workflow_run_id"], "resume", "Old question prose", 2, decision_id="question-A", interaction_owner="monitor")

    # then
    assert record_bytes(store, run["workflow_run_id"]) == before_record
    assert journal_bytes(store, run["workflow_run_id"]) == before_events


def test_markerless_legacy_suspension_has_no_fallback_and_refuses_without_writes(store, tmp_path):
    # given
    run = suspended(store, tmp_path)
    legacy = dict(run)
    legacy.pop("attention_checkpoint", None)
    from harflow_mcp import state_ownership
    with state_ownership.state_session(store.state_layout):
        write_run(store, legacy)
    before_record = record_bytes(store, run["workflow_run_id"])
    before_events = journal_bytes(store, run["workflow_run_id"])
    assert workflow_attention.checkpoint_id(store, store.get_run(run["workflow_run_id"])) is None

    # when
    with pytest.raises(workflow_attention.CheckpointError) as raised:
        store.control(run["workflow_run_id"], "resume", "Do not apply this", 1,
                      attention_checkpoint_id="attention:legacy:fallback", interaction_owner="monitor")

    # then
    assert raised.value.code == "attention_checkpoint_unavailable"
    assert record_bytes(store, run["workflow_run_id"]) == before_record
    assert journal_bytes(store, run["workflow_run_id"]) == before_events


def test_cli_and_public_mcp_advertise_the_optional_guarded_recovery_argument():
    # given
    resume_parser = next(parser for parser in ctl._build_parser() if parser.prog.endswith("workflow-resume"))
    recover_parser = next(parser for parser in ctl._build_parser() if parser.prog.endswith("workflow-recover"))

    # when
    resume_args = resume_parser.parse_args(["run", "--instructions", "answer", "--attention-checkpoint-id", "attention:run:x"])
    recover_args = recover_parser.parse_args(["run", "--reason", "repair", "--attention-checkpoint-id", "attention:run:x"])

    # then
    assert resume_args.attention_checkpoint_id == "attention:run:x"
    assert recover_args.attention_checkpoint_id == "attention:run:x"


async def test_public_mcp_workflow_controls_expose_checkpoint_without_changing_question_path():
    # given
    from mcp import Client
    async with Client(server.mcp) as client:
        tools = {tool.name: tool for tool in (await client.list_tools()).tools}

    # when
    resume_properties = tools["resume_workflow"].input_schema["properties"]
    recover_properties = tools["recover_workflow"].input_schema["properties"]

    # then
    assert "attention_checkpoint_id" in resume_properties
    assert "attention_checkpoint_id" in recover_properties
    assert "decision_id" in resume_properties


def test_issued_attempt_identity_rotates_checkpoint_but_progress_does_not(store, tmp_path):
    run = suspended(store, tmp_path)
    run_id = run["workflow_run_id"]
    initial = workflow_attention.checkpoint_id(store, run)
    assert initial

    progressed = store.update_run(
        run_id,
        lambda value: value["activations"].append({
            "id": "issued-activation", "node_id": "work", "role": "node", "status": "running", "created_at": 0.0, "tasks": [{"task_id": "task-1", "attempt": 1, "status": "running"}],
        }),
        "issued_attempt",
    )
    issued = workflow_attention.checkpoint_id(store, progressed)
    assert issued and issued != initial

    progress = store.update_run(
        run_id,
        lambda value: value["activations"][0]["tasks"][0].update(progress="still running"),
        "attempt_progress",
    )
    assert workflow_attention.checkpoint_id(store, progress) == issued


def test_authoritative_source_marker_rotates_root_token_and_cached_marker_is_ignored(store, tmp_path):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run

    parent = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, parent, parent["activations"][0], status="paused")
    child["attention_checkpoint"] = 7
    from harflow_mcp import state_ownership
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    child_id = child["workflow_run_id"]
    source = {
        "workflow_run_id": child_id,
        "workflow_name": child["name"],
        "path": [parent["workflow_run_id"], child_id],
        "attention_checkpoint": 999999,
    }
    root = store.update_run(parent["workflow_run_id"], lambda value: value.update(
        status="paused", attention_checkpoint=2, attention_source=source,
    ), "forwarded_attention")

    first = workflow_attention.checkpoint_id(store, root)
    assert first and first.startswith("attention:")
    # A source marker copied into the forwarding pointer is diagnostic only.
    source["attention_checkpoint"] = 999998
    changed_cached_pointer = store.update_run(parent["workflow_run_id"], lambda value: value.update(attention_source=source), "cached_marker_changed")
    assert workflow_attention.checkpoint_id(store, changed_cached_pointer) == first

    store.update_run(child_id, lambda value: value.update(attention_checkpoint=8), "new_source_checkpoint")
    refreshed_root = store.get_run(parent["workflow_run_id"])
    assert workflow_attention.checkpoint_id(store, refreshed_root) != first


def test_scheduler_checkpoint_binds_owner_and_rejects_before_effects(store, tmp_path, monkeypatch):
    run = suspended(store, tmp_path)
    run_id = run["workflow_run_id"]
    owner = {"pid": 12345, "start_time": "owner-a", "markers": []}
    active = store.update_run(run_id, lambda value: value.update(
        status="running", supervisor_identity=owner, supervisor_pid=owner["pid"], interaction_owner="monitor",
    ), "scheduler_started")
    checkpoint = workflow_attention.checkpoint_id(store, active)
    assert checkpoint and checkpoint.startswith("scheduler:")
    assert workflow_attention.checkpoint_id(store, {**active, "supervisor_identity": {**owner, "start_time": "owner-b"}}) != checkpoint

    store.update_run(run_id, lambda value: value.update(supervisor_identity={**owner, "start_time": "owner-b"}), "owner_replaced")
    before = record_bytes(store, run_id)
    before_events = journal_bytes(store, run_id)
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: pytest.fail("stale scheduler token must not launch"))
    with pytest.raises(workflow_attention.CheckpointError) as stale:
        store.control(run_id, "recover", "Do not apply", attention_checkpoint_id=checkpoint, interaction_owner="monitor")
    assert stale.value.code == "attention_checkpoint_stale"
    assert record_bytes(store, run_id) == before
    assert journal_bytes(store, run_id) == before_events


def test_scheduler_checkpoint_wrong_mode_is_typed_stale_without_mutation(store, tmp_path):
    run = suspended(store, tmp_path)
    checkpoint = workflow_attention.checkpoint_id(store, run)
    before = record_bytes(store, run["workflow_run_id"])
    before_events = journal_bytes(store, run["workflow_run_id"])
    with pytest.raises(workflow_attention.CheckpointError) as stale:
        store.control(run["workflow_run_id"], "recover", "Do not apply", attention_checkpoint_id="scheduler:wrong-mode", interaction_owner="monitor")
    assert stale.value.code == "attention_checkpoint_stale"
    assert record_bytes(store, run["workflow_run_id"]) == before
    assert journal_bytes(store, run["workflow_run_id"]) == before_events



def test_concurrent_source_switch_serializes_before_guarded_control(store, tmp_path, monkeypatch):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership, workflow_invocation

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    root_id = root["workflow_run_id"]
    source_a = finished_child(store, root, root["activations"][0], status="paused")
    with state_ownership.state_session(store.state_layout):
        write_run(store, source_a)
    second = copy.deepcopy(root["activations"][0])
    second["id"] = uuid.uuid4().hex
    second["invocation"]["child_workflow_run_id"] = uuid.uuid4().hex
    second["token"]["id"] = uuid.uuid4().hex
    second["token"]["execution_activation_id"] = second["id"]
    root = store.update_run(root_id, lambda value: value["activations"].append(second), "second_source")
    source_b = finished_child(store, root, second, status="paused")
    with state_ownership.state_session(store.state_layout):
        write_run(store, source_b)
    source_a = store.update_run(source_a["workflow_run_id"], lambda value: value.update(attention_reason="Checkpoint A"), "source_A")
    source_b = store.update_run(source_b["workflow_run_id"], lambda value: value.update(attention_reason="Checkpoint B"), "source_B")
    workflow_invocation.publish_attention(store, source_a)
    displayed = workflow_attention.checkpoint_id(store, store.get_run(root_id))
    assert displayed
    source_b_id = source_b["workflow_run_id"]
    source_b_before = record_bytes(store, source_b_id)
    launches = []
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: launches.append(args[1]))

    tree_lock = store.tree_mutation
    contention = threading.Event()
    result = []

    def tracked_tree_lock(identifier):
        if threading.current_thread().name == "old-checkpoint-control":
            contention.set()
        return tree_lock(identifier)

    monkeypatch.setattr(store, "tree_mutation", tracked_tree_lock)

    def submit_a():
        try:
            with state_ownership.state_session(store.state_layout):
                store.control(root_id, "resume", "Answer to source A", 2, attention_checkpoint_id=displayed)
        except BaseException as exc:
            result.append(exc)

    with tree_lock(root_id):
        controller = threading.Thread(target=submit_a, name="old-checkpoint-control")
        controller.start()
        assert contention.wait(5)
        # Publish B while the stale A control is already contending on the same tree lock.
        workflow_invocation.publish_attention(store, source_b)
    controller.join(5)
    assert not controller.is_alive()
    assert len(result) == 1 and isinstance(result[0], workflow_attention.CheckpointError)
    assert result[0].code == "attention_checkpoint_stale"
    assert store.get_run(root_id)["attention_source"]["workflow_run_id"] == source_b_id
    assert record_bytes(store, source_b_id) == source_b_before
    current_b = store.get_run(source_b_id)
    assert current_b["status"] == "paused"
    assert current_b.get("instructions", "") == ""
    assert current_b.get("transition_grant", 0) == 0
    assert launches == []


def test_stale_token_preserves_existing_delivery_instructions_grants_and_events(store, tmp_path, monkeypatch):
    run = suspended(store, tmp_path)
    run_id = run["workflow_run_id"]
    displayed = workflow_attention.checkpoint_id(store, run)
    assert displayed
    store.update_run(run_id, lambda value: value.update(
        instructions="Previously accepted", retry_grants={"work": 3},
        forwarded_delivery={"id": "delivery-A", "payload": {
            "source_id": run_id, "action": "resume", "instructions": "Previously accepted",
            "additional_attempts": 3, "decision_id": None, "allow_optional_review_skip": False,
            "interaction_owner": None, "source": {"workflow_run_id": run_id, "workflow_name": value["name"], "path": [run_id]},
            "attention_checkpoint_id": displayed,
        }},
    ), "existing_delivery")
    store.update_run(run_id, lambda value: value.update(attention_reason="Checkpoint B"), "source_switch")
    before_record = record_bytes(store, run_id)
    before_events = journal_bytes(store, run_id)
    launches = []
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: launches.append(args[1]))
    with pytest.raises(workflow_attention.CheckpointError) as stale:
        store.control(run_id, "resume", "New unreviewed prose", 9, attention_checkpoint_id=displayed)
    assert stale.value.code == "attention_checkpoint_stale"
    assert record_bytes(store, run_id) == before_record
    assert journal_bytes(store, run_id) == before_events
    assert store.get_run(run_id)["instructions"] == "Previously accepted"
    assert store.get_run(run_id)["retry_grants"] == {"work": 3}
    assert store.get_run(run_id)["forwarded_delivery"]["id"] == "delivery-A"
    assert launches == []



def test_nested_source_reconciliation_rechecks_before_answer_or_grants(store, tmp_path, monkeypatch):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status="paused")
    child["attention_checkpoint"] = 4
    child["attention_reason"] = "Checkpoint A"
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    child_id = child["workflow_run_id"]
    source = {"workflow_run_id": child_id, "workflow_name": child["name"], "path": [root["workflow_run_id"], child_id]}
    root = store.update_run(root["workflow_run_id"], lambda value: value.update(
        status="paused", attention_checkpoint=2, attention_source=source,
    ), "source_selected")
    displayed = workflow_attention.checkpoint_id(store, root)
    assert displayed
    child_before_events = journal_bytes(store, child_id)
    original_reconcile = store.reconcile_run

    def reconcile(identifier, **options):
        if identifier == child_id:
            return store.update_run(identifier, lambda value: value.update(attention_reason="Checkpoint B"), "truth_reconciled")
        if identifier == root["workflow_run_id"]:
            return store.get_run(identifier)
        return original_reconcile(identifier, **options)

    monkeypatch.setattr(store, "reconcile_run", reconcile)
    launches = []
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: launches.append(args[1]))
    root_before = record_bytes(store, root["workflow_run_id"])
    with pytest.raises(workflow_attention.CheckpointError) as stale:
        store.control(root["workflow_run_id"], "resume", "Old child answer", 5, attention_checkpoint_id=displayed)
    assert stale.value.code == "attention_checkpoint_stale"
    assert store.get_run(child_id)["attention_reason"] == "Checkpoint B"
    assert journal_bytes(store, child_id) != child_before_events
    assert record_bytes(store, root["workflow_run_id"]) == root_before
    assert store.get_run(child_id).get("instructions", "") == ""
    assert store.get_run(child_id).get("retry_grants", {}) == {}
    assert launches == []


@pytest.mark.parametrize(("root_status", "child_status", "wrong_action"), [
    ("paused", "paused", "recover"),
    ("failed", "failed", "resume"),
])
def test_forwarded_control_action_must_match_current_checkpoint_mode(store, tmp_path, root_status, child_status, wrong_action):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status=child_status)
    child["attention_checkpoint"] = 4
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    source = {"workflow_run_id": child["workflow_run_id"], "workflow_name": child["name"],
              "path": [root["workflow_run_id"], child["workflow_run_id"]]}
    root = store.update_run(root["workflow_run_id"], lambda value: value.update(
        status=root_status, attention_checkpoint=2, attention_source=source,
        **({"failure_reason": "Failed root"} if root_status == "failed" else {}),
    ), "forwarded_checkpoint")
    token = workflow_attention.checkpoint_id(store, root)
    assert token
    before = record_bytes(store, root["workflow_run_id"])
    with pytest.raises(workflow_attention.CheckpointError) as mismatch:
        store.control(root["workflow_run_id"], wrong_action, "Wrong action", attention_checkpoint_id=token)
    assert mismatch.value.code == "attention_checkpoint_stale"
    assert record_bytes(store, root["workflow_run_id"]) == before



def test_new_checkpoint_with_same_prose_does_not_acknowledge_old_received_delivery(store, tmp_path, monkeypatch):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership, workflow_invocation

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status="paused")
    child["attention_checkpoint"] = 5
    child["attention_reason"] = "Checkpoint A"
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    child_id = child["workflow_run_id"]
    source = {"workflow_run_id": child_id, "workflow_name": child["name"], "path": [root["workflow_run_id"], child_id]}
    store.update_run(root["workflow_run_id"], lambda value: value.update(
        status="needs_attention", attention_checkpoint=2, attention_source=source,
    ), "source_selected")
    token_a = workflow_attention.checkpoint_id(store, store.get_run(root["workflow_run_id"]))
    assert token_a
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: None)

    store.control(root["workflow_run_id"], "resume", "Same reviewed prose", 2, attention_checkpoint_id=token_a)
    accepted = store.get_run(child_id)
    assert accepted["instructions"] == "Same reviewed prose"
    assert accepted["transition_grant"] == 2

    suspended_again = store.update_run(child_id, lambda value: value.update(
        status="paused", attention_reason="Checkpoint B",
    ), "new_suspension")
    workflow_invocation.publish_attention(store, suspended_again)
    token_b = workflow_attention.checkpoint_id(store, store.get_run(root["workflow_run_id"]))
    assert token_b and token_b != token_a

    resumed_b = store.control(root["workflow_run_id"], "resume", "Same reviewed prose", 3, attention_checkpoint_id=token_b)
    assert resumed_b["status"] == "running"
    child_b = store.get_run(child_id)
    assert child_b["status"] == "running"
    assert child_b["instructions"] == "Same reviewed prose"
    assert child_b["transition_grant"] == 5



def test_public_mcp_and_ctl_forward_checkpoint_and_preserve_typed_stale_error(store, tmp_path, monkeypatch, capsys):
    run = suspended(store, tmp_path)
    run_id = run["workflow_run_id"]
    old = workflow_attention.checkpoint_id(store, run)
    store.update_run(run_id, lambda value: value.update(attention_reason="Checkpoint B"), "new_checkpoint")
    calls = []

    async def mcp_call(action, **kwargs):
        calls.append((action, kwargs))
        try:
            return store.control(kwargs["run_id"], action, kwargs.get("instructions"), kwargs.get("additional_attempts", 0), attention_checkpoint_id=kwargs.get("attention_checkpoint_id"))
        except workflow_attention.CheckpointError as exc:
            raise server.MCPError(-32602, str(exc), data={"code": exc.code}) from None

    monkeypatch.setattr(server, "_workflow_call", mcp_call)
    with pytest.raises(server.MCPError) as mcp_stale:
        import asyncio
        asyncio.run(server.resume_workflow(run_id, instructions="Old prose", additional_attempts=4, attention_checkpoint_id=old))
    assert mcp_stale.value.error.data["code"] == "attention_checkpoint_stale"
    assert calls[-1][1]["attention_checkpoint_id"] == old

    parser = next(parser for parser in ctl._build_parser() if parser.prog.endswith("workflow-resume"))
    args = parser.parse_args([run_id, "--instructions", "Old prose", "--additional-attempts", "4", "--attention-checkpoint-id", old, "--json"])
    args.command = "workflow-resume"
    assert ctl._cmd_workflow(args) == 1
    emitted = json.loads(capsys.readouterr().out)
    assert emitted["error"]["code"] == "attention_checkpoint_stale"
    assert calls[-1][1]["attention_checkpoint_id"] == old



def test_unreceived_superseded_delivery_is_replaced_by_current_checkpoint(store, tmp_path, monkeypatch):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership, workflow_invocation

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status="paused")
    child["attention_checkpoint"] = 3
    child["attention_reason"] = "Checkpoint A"
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    child_id = child["workflow_run_id"]
    source = {"workflow_run_id": child_id, "workflow_name": child["name"], "path": [root["workflow_run_id"], child_id]}
    store.update_run(root["workflow_run_id"], lambda value: value.update(
        status="needs_attention", attention_checkpoint=2, attention_source=source,
    ), "source_selected")
    token_a = workflow_attention.checkpoint_id(store, store.get_run(root["workflow_run_id"]))
    assert token_a
    store.update_run(root["workflow_run_id"], lambda value: value.update(forwarded_delivery={
        "id": "prepared-A", "payload": {
            "source_id": child_id, "action": "resume", "instructions": "Prose A",
            "additional_attempts": 1, "decision_id": None, "allow_optional_review_skip": False,
            "interaction_owner": None, "source": source, "attention_checkpoint_id": token_a,
        },
    }), "prepared_delivery")
    next_child = store.update_run(child_id, lambda value: value.update(
        status="paused", attention_reason="Checkpoint B",
    ), "new_suspension")
    workflow_invocation.publish_attention(store, next_child)
    token_b = workflow_attention.checkpoint_id(store, store.get_run(root["workflow_run_id"]))
    assert token_b and token_b != token_a
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: None)

    result = store.control(root["workflow_run_id"], "resume", "Prose B", 2, attention_checkpoint_id=token_b)
    accepted = store.get_run(child_id)
    assert result["status"] == "running"
    assert "forwarded_delivery" not in result
    assert accepted["instructions"] == "Prose B"
    assert accepted["transition_grant"] == 2
    assert accepted.get("input_delivery_receipt", {}).get("delivery_id") != "prepared-A"



def test_unverified_wrong_root_child_has_no_actionable_token_and_guard_refuses_without_writes(store, tmp_path):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership, workflow_inputs

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status="paused")
    child["attention_checkpoint"] = 7
    child["parent_link"]["root_workflow_run_id"] = uuid.uuid4().hex
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    child_id = child["workflow_run_id"]
    source = {"workflow_run_id": child_id, "workflow_name": child["name"],
              "path": [root["workflow_run_id"], child_id]}
    root = store.update_run(root["workflow_run_id"], lambda value: value.update(
        status="paused", attention_checkpoint=2, attention_source=source,
    ), "unverified_source")

    projection = workflow_inputs.project(store, root)
    diagnostic = next(row for row in projection["pending_attention"] if row["workflow_run_id"] == child_id)
    assert diagnostic.get("parent_workflow_run_id") == root["workflow_run_id"]
    assert "attention_checkpoint_id" not in diagnostic
    assert workflow_attention.checkpoint_id(store, root) is None

    root_before = record_bytes(store, root["workflow_run_id"])
    child_before = record_bytes(store, child_id)
    root_events = journal_bytes(store, root["workflow_run_id"])
    with pytest.raises(workflow_attention.CheckpointError) as unavailable:
        store.control(root["workflow_run_id"], "resume", "Unverified child answer", 3,
                      attention_checkpoint_id="attention:unverified")
    assert unavailable.value.code == "attention_checkpoint_unavailable"
    assert record_bytes(store, root["workflow_run_id"]) == root_before
    assert record_bytes(store, child_id) == child_before
    assert journal_bytes(store, root["workflow_run_id"]) == root_events


@pytest.mark.parametrize(("root_status", "child_status", "wrong_action"), [
    ("paused", "paused", "recover"),
    ("failed", "failed", "resume"),
])
def test_unguarded_forwarded_wrong_action_refuses_before_delivery(store, tmp_path, root_status, child_status, wrong_action):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status=child_status)
    child["attention_checkpoint"] = 8
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    source = {"workflow_run_id": child["workflow_run_id"], "workflow_name": child["name"],
              "path": [root["workflow_run_id"], child["workflow_run_id"]]}
    root = store.update_run(root["workflow_run_id"], lambda value: value.update(
        status=root_status, attention_checkpoint=3, attention_source=source,
        **({"failure_reason": "Failed root"} if root_status == "failed" else {}),
    ), "forwarded_checkpoint")
    before_root = record_bytes(store, root["workflow_run_id"])
    before_child = record_bytes(store, child["workflow_run_id"])
    before_events = journal_bytes(store, root["workflow_run_id"])
    with pytest.raises(workflows.WorkflowError):
        store.control(root["workflow_run_id"], wrong_action, "Do not deliver", 2)
    assert record_bytes(store, root["workflow_run_id"]) == before_root
    assert record_bytes(store, child["workflow_run_id"]) == before_child
    assert journal_bytes(store, root["workflow_run_id"]) == before_events



def test_guarded_recover_of_forwarded_failed_child_remains_supported(store, tmp_path, monkeypatch):
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership

    root = seed_parent(store, tmp_path, "parent", stage="waiting")
    child = finished_child(store, root, root["activations"][0], status="failed", failed_decision="failed-checkpoint")
    child["attention_checkpoint"] = 9
    with state_ownership.state_session(store.state_layout):
        write_run(store, child)
    source = {"workflow_run_id": child["workflow_run_id"], "workflow_name": child["name"],
              "path": [root["workflow_run_id"], child["workflow_run_id"]]}
    root = store.update_run(root["workflow_run_id"], lambda value: value.update(
        status="failed", failure_reason="Child failed", attention_checkpoint=4, attention_source=source,
    ), "forwarded_failure")
    token = workflow_attention.checkpoint_id(store, root)
    assert token
    monkeypatch.setattr(workflows, "_request_launch", lambda *args, **kwargs: None)

    recovered = store.control(root["workflow_run_id"], "recover", "Inspected child failure", 1,
                              attention_checkpoint_id=token)
    recovered_child = store.get_run(child["workflow_run_id"])
    assert recovered["status"] == "running"
    assert recovered_child["status"] == "running"
    assert recovered_child["instructions"] == "Inspected child failure"
    assert recovered_child["transition_grant"] == 1



def _nested_attention_tree(store, tmp_path):
    from test_workflow_run_node_execution import child_graph, parent_graph
    from test_workflow_run_node_recovery import finished_child, seed_parent, write_run
    from harflow_mcp import state_ownership

    leaf_def = store.save("nested-leaf", child_graph("nested-leaf"))
    mid_def = store.save("nested-mid", parent_graph(leaf_def["workflow_id"]))
    store.save("nested-root", parent_graph(mid_def["workflow_id"]))
    root = seed_parent(store, tmp_path, "nested-root", stage="waiting")
    root_id = root["workflow_run_id"]

    mid_shell = finished_child(store, root, root["activations"][0], status="paused")
    mid_id = mid_shell["workflow_run_id"]
    mid_link = copy.deepcopy(mid_shell["parent_link"])
    mid_template = seed_parent(store, tmp_path, "nested-mid", stage="waiting")
    mid = copy.deepcopy(mid_template)
    mid["workflow_run_id"] = mid_id
    mid["parent_link"] = mid_link
    mid["orchestrator_mode"] = mid_shell.get("orchestrator_mode", "child")
    mid["status"] = "needs_attention"
    mid["attention_checkpoint"] = 20
    mid["attention_reason"] = "Mid checkpoint"

    leaf = finished_child(store, mid, mid["activations"][0], status="paused")
    leaf_id = leaf["workflow_run_id"]
    leaf["attention_checkpoint"] = 30
    leaf["attention_reason"] = "Leaf checkpoint"
    mid["attention_source"] = {"workflow_run_id": leaf_id, "workflow_name": leaf["name"],
                                "path": [mid_id, leaf_id]}
    root["status"] = "needs_attention"
    root["attention_checkpoint"] = 10
    root["attention_reason"] = "Root checkpoint"
    root["interaction_owner"] = "monitor"
    root["attention_source"] = {"workflow_run_id": mid_id, "workflow_name": mid["name"],
                                "path": [root_id, mid_id]}
    with state_ownership.state_session(store.state_layout):
        write_run(store, leaf)
        write_run(store, mid)
        write_run(store, root)
    token = workflow_attention.checkpoint_id(store, root)
    assert token
    return root, mid, leaf, token



def test_nested_received_delivery_completes_mid_and_root_using_original_token(store, tmp_path, monkeypatch):
    root, mid, leaf, token = _nested_attention_tree(store, tmp_path)
    ids = [root["workflow_run_id"], mid["workflow_run_id"], leaf["workflow_run_id"]]
    original_update = store.update_run
    injected = {"value": False}

    def fail_after_leaf_receipt(identifier, change, event, detail=None, **options):
        if identifier == ids[2] and event == "control:resume" and not injected["value"]:
            injected["value"] = True
            original_update(identifier, change, event, detail, **options)
            raise OSError("injected crash after leaf receipt")
        return original_update(identifier, change, event, detail, **options)

    monkeypatch.setattr(store, "update_run", fail_after_leaf_receipt)
    with pytest.raises(OSError, match="after leaf receipt"):
        store.control(ids[0], "resume", "Nested reviewed prose", 2, attention_checkpoint_id=token)
    assert injected["value"]

    restarted = workflows.WorkflowStore(root=store.root)
    assert workflow_attention.checkpoint_id(restarted, restarted.get_run(ids[0])) == token
    from harflow_mcp import workflow_cancellation, workflow_reads
    projected = workflow_reads.on_demand_projection(restarted, restarted.get_run(ids[0]))
    assert projected["attention_checkpoint_id"] == token
    assert workflow_cancellation.monitor_projection(restarted, projected)["monitor_controls"]["resume"]["allowed"]
    leaf_before = record_bytes(restarted, ids[2])
    monkeypatch.setattr(restarted, "reconcile_run", lambda identifier, **options: restarted.get_run(identifier))
    launches = []
    monkeypatch.setattr(workflows, "_launch", lambda storage, identifier: launches.append(identifier))
    completed = restarted.control(ids[0], "resume", "Nested reviewed prose", 2, attention_checkpoint_id=token)

    assert completed["status"] == "running" and "forwarded_delivery" not in completed
    mid_after = restarted.get_run(ids[1])
    leaf_after = restarted.get_run(ids[2])
    assert mid_after["status"] == "running" and "forwarded_delivery" not in mid_after
    assert mid_after["input_delivery_receipt"]["workflow_run_id"] == ids[2]
    assert leaf_after["status"] == "running"
    assert leaf_after["instructions"] == "Nested reviewed prose"
    assert leaf_after["transition_grant"] == 2
    assert record_bytes(restarted, ids[2]) == leaf_before
    assert launches == [ids[0]]


@pytest.mark.parametrize("damage", ["mid_witness", "leaf_reason"])
def test_nested_received_delivery_mismatch_keeps_original_request_stale(store, tmp_path, monkeypatch, damage):
    root, mid, leaf, token = _nested_attention_tree(store, tmp_path)
    ids = [root["workflow_run_id"], mid["workflow_run_id"], leaf["workflow_run_id"]]
    original = store.update_run
    injected = False

    def fail_after_leaf_receipt(identifier, change, event, detail=None, **options):
        nonlocal injected
        if identifier == ids[2] and event == "control:resume" and not injected:
            injected = True
            original(identifier, change, event, detail, **options)
            raise OSError("injected crash after leaf receipt")
        return original(identifier, change, event, detail, **options)

    monkeypatch.setattr(store, "update_run", fail_after_leaf_receipt)
    with pytest.raises(OSError, match="after leaf receipt"):
        store.control(ids[0], "resume", "Nested reviewed prose", 2, attention_checkpoint_id=token)
    restarted = workflows.WorkflowStore(root=store.root)
    if damage == "mid_witness":
        mid_record = restarted.get_run(ids[1])
        delivery = mid_record["forwarded_delivery"]
        delivery["payload"]["attention_root_checkpoint"] += 1
        restarted.update_run(ids[1], lambda value: value.update(forwarded_delivery=delivery), "corrupt_witness")
    else:
        changed_leaf = restarted.update_run(ids[2], lambda value: value.update(
            status="paused", attention_reason="New leaf checkpoint",
        ), "new_leaf_checkpoint")
        from harflow_mcp import workflow_invocation
        workflow_invocation.publish_attention(restarted, changed_leaf)
    before = {identifier: record_bytes(restarted, identifier) for identifier in ids}
    monkeypatch.setattr(restarted, "reconcile_run", lambda identifier, **options: restarted.get_run(identifier))
    launches = []
    monkeypatch.setattr(workflows, "_launch", lambda storage, identifier: launches.append(identifier))

    with pytest.raises(workflow_attention.CheckpointError) as stale:
        restarted.control(ids[0], "resume", "Nested reviewed prose", 2, attention_checkpoint_id=token)
    assert stale.value.code == "attention_checkpoint_stale"
    assert {identifier: record_bytes(restarted, identifier) for identifier in ids} == before
    assert restarted.get_run(ids[2])["transition_grant"] == 2
    assert restarted.get_run(ids[2])["instructions"] == "Nested reviewed prose"
    assert launches == []



def test_scheduler_owner_uncertainty_refuses_before_general_reconciliation(store, tmp_path, monkeypatch):
    from harflow_mcp import identity

    run = suspended(store, tmp_path)
    run_id = run["workflow_run_id"]
    owner = {"pid": 123456, "start_time": "uncertain-owner", "markers": []}
    active = store.update_run(run_id, lambda value: value.update(
        status="running", supervisor_pid=owner["pid"], supervisor_identity=owner,
    ), "scheduler_started")
    token = workflow_attention.checkpoint_id(store, active)
    assert token and token.startswith("scheduler:")
    monkeypatch.setattr(identity, "identity_check", lambda evidence: "undecidable")
    monkeypatch.setattr(store, "reconcile_run", lambda *args, **kwargs: pytest.fail("scheduler refusal must precede general reconciliation"))
    before = record_bytes(store, run_id)
    before_events = journal_bytes(store, run_id)

    with pytest.raises(workflows.WorkflowError, match="live or uncertain"):
        store.control(run_id, "recover", "Inspect scheduler", attention_checkpoint_id=token)
    assert record_bytes(store, run_id) == before
    assert journal_bytes(store, run_id) == before_events
