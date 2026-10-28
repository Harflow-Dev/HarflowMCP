"""Caller messaging uses workflow dispatch authority, with durable refusal and atomic claims."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from harflow_mcp import messaging, workflow_messages as m, workflows as w, server, followups
from harflow_mcp import state_ownership
from harflow_mcp.state_layout import resolve_layout
from test_workflow_delegation import graph, Registry, default_decision
from test_followup_queue import put_record


@pytest.fixture
def storage(tmp_path, monkeypatch):
    monkeypatch.setattr(w.backends, "is_installed", lambda backend: True)
    monkeypatch.setattr(w, "_launch", lambda *args: None)
    return w.WorkflowStore(tmp_path)


def create(storage, tmp_path):
    return storage.create_run(w.validate_definition(graph()), "request", tmp_path)


def add_attempt(storage, rid, *, task_id="original", node_id="work", role="node", status="running"):
    def add(run):
        run["activations"].append({"id": "activation-" + task_id, "node_id": node_id, "role": role, "status": status, "created_at": 0.0,
                                  "tasks": [{"task_id": task_id, "candidate": {"backend": "codex"}, "status": status}]})
    return storage.update_run(rid, add, "test")


def reserve(storage, rid, tid="next", node_id="work", role="node"):
    claimed = []
    def take(run):
        activation = {"id": "activation-" + tid, "node_id": node_id, "role": role, "status": "running", "created_at": 0.0,
                      "tasks": [{"task_id": tid, "candidate": {"backend": "codex"}, "status": "reserved"}]}
        run["activations"].append(activation)
        claimed.extend(m.claim(run, activation, tid))
    storage.update_run(rid, take, "test_reserve")
    return claimed


def test_queue_verbatim_cancel_and_safe_now_refusal(storage, tmp_path):
    run = create(storage, tmp_path)
    rid = run["workflow_run_id"]
    add_attempt(storage, rid)
    first = m.send(storage, rid, "  exact\r\ntext  ", task_id="original", delivery="now")
    assert first["refusal"]["code"] == "interrupt_unsupported"
    assert first["status"] == "queued"
    messages = storage.get_run(rid)["messages"]
    assert messages[0]["text"] == "  exact\r\ntext  "
    assert messages[0]["target"] == {"kind": "task", "workflow_run_id": rid, "task_id": "original", "node_id": "work", "role": "node"}
    assert m.cancel(storage, rid, first["message_id"])["status"] == "cancelled"
    assert "text" not in storage.get_run(rid)["messages"][0]


def test_target_settles_message_goes_to_next_node_once(storage, tmp_path):
    rid = create(storage, tmp_path)["workflow_run_id"]
    add_attempt(storage, rid)
    first = m.send(storage, rid, "first", task_id="original")
    second = m.send(storage, rid, "second", task_id="original")
    storage.update_run(rid, lambda run: m.task_state(run, "original", "completed"), "test_settled")
    assert reserve(storage, rid, "other", node_id="end", role="orchestrator") == []
    claimed = reserve(storage, rid)
    assert [msg["id"] for msg in claimed] == [first["message_id"], second["message_id"]]
    assert [msg["text"] for msg in claimed] == ["first", "second"]
    with pytest.raises(Exception, match="reserved"):
        m.cancel(storage, rid, first["message_id"])
    storage.update_run(rid, lambda run: m.task_state(run, "next", "running"), "test_started")
    assert reserve(storage, rid, "later") == []
    assert [msg["status"] for msg in storage.get_run(rid)["messages"]] == ["delivered", "delivered"]


def test_positive_no_spawn_releases_but_uncertain_does_not(storage, tmp_path):
    rid = create(storage, tmp_path)["workflow_run_id"]
    first = m.send(storage, rid, "run context")
    assert reserve(storage, rid, "one", node_id="start", role="orchestrator")
    storage.update_run(rid, lambda run: (run["activations"][-1]["tasks"][0].update(status="uncertain"), m.task_state(run, "one", "uncertain")), "test_uncertain")
    assert reserve(storage, rid, "two", node_id="start", role="orchestrator") == []
    storage.update_run(rid, lambda run: (run["activations"][0]["tasks"][0].update(status="not_started"), m.task_state(run, "one", "not_started")), "test_never_started")
    again = reserve(storage, rid, "three", node_id="start", role="orchestrator")
    assert [item["id"] for item in again] == [first["message_id"]]


def test_concurrent_queue_and_claim_are_ordered_and_unique(storage, tmp_path):
    rid = create(storage, tmp_path)["workflow_run_id"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        replies = list(pool.map(lambda i: m.send(w.WorkflowStore(storage.root), rid, str(i)), range(8)))
    assert sorted(reply["seq"] for reply in replies) == list(range(1, 9))
    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(lambda tid: reserve(w.WorkflowStore(storage.root), rid, tid, "start", "orchestrator"), ["a", "b"]))
    assert sorted(len(items) for items in claims) == [0, 8]
    assert [item["seq"] for item in max(claims, key=len)] == list(range(1, 9))


def test_owner_refusal_does_not_enqueue_or_change_goal(storage, tmp_path):
    rid = create(storage, tmp_path)["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(interaction_owner="monitor"), "test")
    before = storage.get_run(rid)
    with pytest.raises(Exception, match="original caller"):
        m.send(storage, rid, "do more", interaction_owner="caller")
    assert storage.get_run(rid) == before
    accepted = m.send(storage, rid, "do more", interaction_owner="monitor")
    assert accepted["status"] == "queued"
    after = storage.get_run(rid)
    for key in ("definition", "goal", "freedom", "network", "transitions"):
        assert after.get(key) == before.get(key)


async def test_real_workflow_dispatch_consumes_run_messages_once(storage, tmp_path):
    run = create(storage, tmp_path)
    rid = run["workflow_run_id"]
    m.send(storage, rid, "  supplementary context\n  ")
    registry = Registry(storage.root)
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    final = storage.get_run(rid)
    assert final["status"] == "completed", final.get("attention_reason")
    assert sum("  supplementary context\n  " in prompt for prompt, _ in registry.calls) == 1
    assert final["messages"][0]["status"] == "delivered"


async def test_standalone_queue_live_closed_and_now_refusal(tmp_path, monkeypatch):
    from harflow_mcp.tasks import TaskRegistry
    directory = tmp_path / "state" / "tasks"
    with state_ownership.state_session(resolve_layout(root=directory.parent)):
        pass
    registry = TaskRegistry(log_dir=directory)
    put_record(directory, live_input=True, status="running", pid=123456, owner={"pid":123456,"start_time":"then","markers":[]}, exit_code=None, finished_at=None)
    monkeypatch.setattr(messaging.store, "resolve_status", lambda *a, **k: ("running", "", None, []))
    response = await messaging.send(registry, "src", " queued ", delivery="now")
    assert response["refusal"]["code"] == "interrupt_unsupported"
    assert followups.view(directory, "src")["items"][0]["text"] == " queued "
    assert messaging.cancel(directory, "src", response["message_id"])["status"] == "cancelled"


async def test_settled_now_starts_continuation_with_existing_guards(tmp_path, monkeypatch):
    from harflow_mcp.tasks import TaskRegistry
    directory = tmp_path / "state" / "tasks"
    with state_ownership.state_session(resolve_layout(root=directory.parent)):
        pass
    registry = TaskRegistry(log_dir=directory)
    put_record(directory)
    calls = []
    class Successor:
        task_id = "successor"
    async def continued(task_id, **kwargs):
        calls.append((task_id, kwargs))
        return Successor()
    monkeypatch.setattr(registry, "continue_task", continued)
    response = await messaging.send(registry, "src", "continue now", delivery="now")
    assert response["successor_task_id"] == "successor"
    assert calls == [("src", {"consume_followups": True})]


def test_public_cli_parses_shared_routes():
    from harflow_mcp import ctl
    parser = ctl._parser()[0] if hasattr(ctl, "_parser") else ctl._build_parser()[0]
    assert parser.parse_args(["send", "task", "hi", "--delivery", "now", "--monitor"]).delivery == "now"
    parsed = parser.parse_args(["workflow-send", "run", "hello", "--task-id", "task", "--monitor"])
    assert parsed.workflow_run_id == "run" and parsed.task_id == "task"


async def test_message_arriving_during_node_is_delivered_only_to_issued_successor(storage, tmp_path):
    definition = graph()
    definition["connections"].append({"id": "retry", "source": "work", "target": "work", "condition": "One refinement", "max_retries": 1})
    run = storage.create_run(w.validate_definition(definition), "request", tmp_path)
    rid = run["workflow_run_id"]
    storage.update_run(rid, lambda r: r.pop("runner_policy", None), "fixture")
    queued = []
    worker_prompts = []
    def output(prompt, kwargs):
        worker_prompts.append(prompt)
        if not queued:
            queued.append(m.send(storage, rid, "  Follow-up after this worker settles\n", task_id=kwargs["task_id"]))
        return {"status": "succeeded", "result": {"summary": "Done"}, "evidence": ["observed"]}
    def policy(context, registry):
        decision = default_decision(context, registry)
        choices = context["valid_continuations"]
        if any(c["continuation_id"] == "retry" for c in choices) and len(worker_prompts) == 1:
            decision["next"] = [{"continuation_id": "retry", "prompt": "Refine only this node", "session_mode": "fresh"}]
        else:
            decision["next"] = [entry for entry in decision["next"] if entry["continuation_id"] != "retry"]
        return decision
    registry = Registry(storage.root, policy=policy, outputs={"work": output})
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    final = storage.get_run(rid)
    assert final["status"] == "completed", final.get("attention_reason")
    assert len(worker_prompts) == 2
    assert "Follow-up after this worker settles" not in worker_prompts[0]
    assert "  Follow-up after this worker settles\n" in worker_prompts[1]
    assert final["messages"][0]["status"] == "delivered"
    assert final["messages"][0]["delivery_task_id"] != final["messages"][0]["target"]["task_id"]


async def test_unissued_node_followup_becomes_undeliverable_on_completion(storage, tmp_path):
    run = create(storage, tmp_path)
    rid = run["workflow_run_id"]
    queued = []
    def output(prompt, kwargs):
        queued.append(m.send(storage, rid, "future node context", task_id=kwargs["task_id"]))
        return {"status": "succeeded", "result": {"summary": "Done"}, "evidence": ["observed"]}
    registry = Registry(storage.root, outputs={"work": output})
    await w.WorkflowSupervisor(registry, storage).execute(rid)
    message = storage.get_run(rid)["messages"][0]
    assert message["status"] == "undeliverable"
    assert "ended" in message["reason"]


async def test_now_respects_pending_question_and_resumes_safe_pause(storage, tmp_path, monkeypatch):
    rid = create(storage, tmp_path)["workflow_run_id"]
    storage.update_run(rid, lambda r: r.update(status="needs_input", input_question="Choose", input_decision_id="decision"), "fixture")
    response = m.send(storage, rid, "caller context", delivery="now")
    response = await m.deliver_now(storage, response, interaction_owner="caller")
    assert response["refusal"]["code"] == "workflow_checkpoint_required"
    assert storage.get_run(rid)["status"] == "needs_input"
    storage.update_run(rid, lambda r: (r.update(status="paused"), r.pop("input_question"), r.pop("input_decision_id")), "fixture")
    resumed = await m.deliver_now(storage, response, interaction_owner="caller")
    assert resumed["delivery_started"] is True and "refusal" not in resumed
    assert storage.get_run(rid)["status"] in {"starting", "running"}


def test_task_route_and_cancel_follow_the_owning_workflow(storage, tmp_path):
    rid = create(storage, tmp_path)["workflow_run_id"]
    add_attempt(storage, rid)
    response = messaging.queue(storage.root / "tasks", "original", "targeted text")
    assert response["workflow_run_id"] == rid
    assert messaging.cancel(storage.root / "tasks", "original", response["message_id"])["status"] == "cancelled"
    assert not followups.state_path(storage.root / "tasks", "original").exists()


def test_compact_status_retains_message_identity_and_full_detail(storage, tmp_path):
    from harflow_mcp import workflow_responses
    rid = create(storage, tmp_path)["workflow_run_id"]
    response = m.send(storage, rid, "x" * 1000)
    run = storage.get_run(rid)
    compact = workflow_responses.compact(run)
    assert compact["messages"][0]["id"] == response["message_id"]
    assert compact["messages"][0]["text_truncated"] and compact["messages_truncated"]
    assert workflow_responses._view_value(run, "messages")[0]["text"] == "x" * 1000


def test_cancel_racing_claim_has_one_authoritative_outcome(storage, tmp_path):
    rid = create(storage, tmp_path)["workflow_run_id"]
    response = m.send(storage, rid, "only once")
    def cancel():
        try:
            return m.cancel(w.WorkflowStore(storage.root), rid, response["message_id"])["status"]
        except Exception as exc:
            assert getattr(exc, "code", None) == "not_cancellable"
            return "claimed"
    with ThreadPoolExecutor(max_workers=2) as pool:
        cancel_future = pool.submit(cancel)
        claim_future = pool.submit(reserve, w.WorkflowStore(storage.root), rid, "next", "start", "orchestrator")
        state, taken = cancel_future.result(), claim_future.result()
    assert (state == "cancelled" and taken == []) or (state == "claimed" and len(taken) == 1)
    assert storage.get_run(rid)["messages"][0]["status"] in {"cancelled", "delivering"}


async def test_now_launch_refusal_preserves_text_and_queue(tmp_path, monkeypatch):
    from harflow_mcp.tasks import TaskRegistry
    directory = tmp_path / "state" / "tasks"
    with state_ownership.state_session(resolve_layout(root=directory.parent)):
        pass
    registry = TaskRegistry(log_dir=directory)
    put_record(directory)
    async def refused(*args, **kwargs):
        raise ValueError("Approval requires explicit response")
    monkeypatch.setattr(registry, "continue_task", refused)
    response = await messaging.send(registry, "src", " exact now ", delivery="now")
    assert response["status"] == "queued"
    assert response["refusal"]["code"] == "continuation_refused"
    assert followups.view(directory, "src")["items"][0]["text"] == " exact now "


@pytest.mark.parametrize('status', ['completed', 'cancelled'])
def test_terminal_now_is_truthfully_undeliverable(storage, tmp_path, status):
    rid = create(storage, tmp_path)['workflow_run_id']
    storage.update_run(rid, lambda run: run.update(status=status), 'fixture')
    response = m.send(storage, rid, 'late message', delivery='now')
    assert response['status'] == 'undeliverable'
    assert response['refusal']['code'] == 'workflow_terminal'
    assert 'stays queued' not in response['refusal']['message']
    assert response['reason'] == storage.get_run(rid)['messages'][0]['reason']


async def test_builder_now_rechecks_handover_at_atomic_feedback_mutation(storage, tmp_path, monkeypatch):
    from harflow_mcp import identity
    run = storage.create_run(w.validate_definition(graph()), 'builder request', tmp_path, kind='builder', freedom='read_only')
    rid = run['workflow_run_id']
    storage.update_run(rid, lambda value: value.update(status='completed', interaction_caller={'kind':'external_parent','identity':{'pid':42,'start_time':'qualified','markers':[]}}), 'fixture')
    response = m.send(storage, rid, 'caller feedback', delivery='now')
    monkeypatch.setattr(identity, 'check_detail', lambda value: ('dead', 'pid_absent'))
    original = m._authorize
    interleaved = []
    def handover_after_outer_authorization(store, value, responder, action):
        original(store, value, responder, action)
        if responder == 'caller' and not interleaved:
            interleaved.append(True)
            m.send(storage, rid, 'Monitor claimed ownership', interaction_owner='monitor')
    monkeypatch.setattr(m, '_authorize', handover_after_outer_authorization)
    launched = []
    monkeypatch.setattr(w, '_launch', lambda *args: launched.append(args))
    result = await m.deliver_now(storage, response, interaction_owner='caller')
    current = storage.get_run(rid)
    assert result['refusal']['code'] == 'interaction_owned'
    assert 'delivery_started' not in result and launched == []
    assert current['interaction_owner'] == 'monitor' and current['status'] == 'completed'
    assert current['messages'][0]['status'] == 'queued'
    assert not current.get('builder_messages')
    # The legacy public builder-followup route cannot bypass the same handover by omission.
    with pytest.raises(Exception, match='original caller'):
        await w.followup_workflow_builder(rid, 'stale legacy feedback', root=storage.root)


async def test_builder_now_refuses_if_another_turn_started_before_atomic_mutation(storage, tmp_path, monkeypatch):
    run = storage.create_run(w.validate_definition(graph()), 'builder request', tmp_path, kind='builder', freedom='read_only')
    rid = run['workflow_run_id']
    storage.update_run(rid, lambda value: value.update(status='completed'), 'fixture')
    response = m.send(storage, rid, 'caller feedback', delivery='now')
    original = m._authorize
    interleaved = []
    def start_after_outer_authorization(store, value, responder, action):
        original(store, value, responder, action)
        if not interleaved:
            interleaved.append(True)
            add_attempt(storage, rid, task_id='concurrent-builder', node_id='builder', role='builder')
    monkeypatch.setattr(m, '_authorize', start_after_outer_authorization)
    launched = []
    monkeypatch.setattr(w, '_launch', lambda *args: launched.append(args))
    result = await m.deliver_now(storage, response, interaction_owner='caller')
    assert result['refusal']['code'] == 'interrupt_unsupported'
    assert not result.get('delivery_started') and not launched
    assert storage.get_run(rid)['messages'][0]['status'] == 'queued'


async def test_default_public_task_status_keeps_workflow_message_ids(storage, tmp_path, monkeypatch):
    from harflow_mcp.tasks import TaskRegistry
    rid = create(storage, tmp_path)['workflow_run_id']
    add_attempt(storage, rid)
    put_record(storage.root / 'tasks', task_id='original', backend='codex')
    message = m.send(storage, rid, 'node message', task_id='original')
    monkeypatch.setattr(server, '_registry', TaskRegistry(log_dir=storage.root / 'tasks'))
    status = await server.get_task_status('original')
    assert status['messages'][0]['id'] == message['message_id']
    assert status['messages'][0]['status'] == 'queued'


@pytest.mark.parametrize('monitor', [False, True])
def test_builder_cli_records_initial_interaction_owner(storage, tmp_path, monkeypatch, capsys, monitor):
    from harflow_mcp import ctl, takeover
    from harflow_mcp.tasks import TaskRegistry
    monkeypatch.setattr(server, '_registry', TaskRegistry(log_dir=storage.root / 'tasks'))
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: storage.root / 'tasks')
    monkeypatch.setattr(takeover, 'caller_refusal', lambda *args: None)
    import subprocess
    repo = tmp_path / 'builder-repo'
    repo.mkdir()
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    args = ['workflow-build','builder','--prompt','draft','--backend','codex','--repo',str(repo),'--json']
    if monitor:
        args.append('--monitor')
    assert ctl.main(args) == 0
    import json
    rid = json.loads(capsys.readouterr().out)['result']['workflow_run_id']
    assert storage.get_run(rid)['interaction_owner'] == ('monitor' if monitor else 'caller')


def test_builder_monitor_cli_refuses_agent_before_creation(storage, tmp_path, monkeypatch, capsys):
    from harflow_mcp import ctl, takeover
    from harflow_mcp.tasks import TaskRegistry
    monkeypatch.setattr(server, '_registry', TaskRegistry(log_dir=storage.root / 'tasks'))
    monkeypatch.setattr(ctl, 'default_log_dir', lambda: storage.root / 'tasks')
    monkeypatch.setattr(takeover, 'caller_refusal', lambda *args: ('agent_caller', 'A managed agent is not a human Monitor caller'))
    assert ctl.main(['workflow-build','builder','--prompt','draft','--backend','codex','--repo',str(tmp_path),'--monitor','--json']) == 1
    assert storage.list_runs() == []
    assert 'verified human' in capsys.readouterr().out
